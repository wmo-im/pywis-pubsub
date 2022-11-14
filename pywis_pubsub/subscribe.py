###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import base64
import enum
import hashlib
import json
import logging
from pathlib import Path
import urllib

import click

from pywis_pubsub import cli_options
from pywis_pubsub import util
from pywis_pubsub.geometry import is_message_within_bbox
from pywis_pubsub.storage import STORAGES
from pywis_pubsub.validation import validate_message
from pywis_pubsub.mqtt import MQTTPubSubClient


LOGGER = logging.getLogger(__name__)


class VerificationMethods(enum.Enum):
    md5 = 'md5'
    sha512 = 'sha512'


def get_canonical_link(links: list):
    """
    Helper function to derive canonical link from a list of link objects

    :param links: `list` of link `dict`s

    :returns: `dict` of first canonical link object found
    """

    try:
        return list(filter(lambda d: d['rel'] == 'canonical', links))[0]
    except IndexError:
        LOGGER.error('No canonical link found')
        return {}


def get_data(msg_dict: dict) -> bytes:
    """
    Data downloading functionality

    :param msg_dict: `dict` of notification message

    :returns: `bytes` of data
    """

    canonical_link = get_canonical_link(msg_dict['links'])

    if canonical_link:
        LOGGER.debug(f'Found canonical link: {canonical_link}')

    props = msg_dict['properties']

    if 'content' in props and 'value' in props['content']:
        LOGGER.debug('Decoding from inline data')
        data = base64.b64decode(props['content']['value'])
    else:
        LOGGER.debug(f"Downloading from {canonical_link['href']}")
        try:
            with urllib.request.urlopen(canonical_link['href']) as f:
                data = f.read()
        except urllib.error.HTTPError as err:
            LOGGER.error(f"download error ({canonical_link['href']}): {err}")
            raise

    return data


def data_verified(data: bytes, size: int, method: VerificationMethods,
                  value: str):
    """
    Verify integrity of data given a hashing method

    :param data: data to verify
    :param size: `int` size of data
    :param method: method of verification (`VerificationMethods`)
                   default is sha512
    :param value: value of hashing result

    :returns: `bool` of whether data is verified
    """

    LOGGER.debug(f'Verification method is {method}')
    data_value = getattr(hashlib, method)(data).hexdigest()

    LOGGER.debug('Comparing checksum and data size')
    return data_value == value and len(data) == size


def on_message_handler(client, userdata, msg):
    """message handler"""

    msg_dict = json.loads(msg.payload)
    msg_json = json.dumps(msg_dict, indent=4)

    try:
        if userdata.get('validate_message', False):
            LOGGER.debug('Validating message')
            success, err = validate_message(msg_dict)
            if not success:
                LOGGER.error(f'Message is not a valid notification: {err}')
                return
    except RuntimeError as err:
        LOGGER.error(f'Cannot validate message: {err}')
        return

    LOGGER.debug(f'Topic: {msg.topic}')
    LOGGER.debug(f'Raw message:\n{msg_json}')

    if userdata.get('bbox') and msg_dict.get('geometry') is not None:
        LOGGER.debug('Performing spatial filtering')
        if is_message_within_bbox(msg_dict['geometry'], userdata['bbox']):
            LOGGER.debug('Message geometry is within bbox')
        else:
            LOGGER.debug('Message geometry not within bbox; skipping')
            return

    clink = get_canonical_link(msg_dict['links'])
    LOGGER.info(f"Received message with data-url {clink['href']}")

    if userdata.get('storage') is not None:
        LOGGER.debug('Saving data')
        try:
            LOGGER.debug('Downloading data')
            data = get_data(msg_dict)
        except Exception as err:
            LOGGER.error(err)
            return
        if ('integrity' in msg_dict['properties'] and
                userdata.get('verify_data', True)):
            LOGGER.debug('Verifying data')

            method = msg_dict['properties']['integrity']['method']
            value = msg_dict['properties']['integrity']['value']
            if 'content' in msg_dict['properties']:
                size = msg_dict['properties']['content']['size']
            else:
                size = clink['length']

            if not data_verified(data, size, method, value):
                LOGGER.error('Data verification failed; not saving')
                return
            else:
                LOGGER.debug('Data verification passed')

        link = Path(clink['href'])
        filename = link.name

        storage_class = STORAGES[userdata.get('storage').get('type')]
        storage_object = storage_class(userdata['storage'])
        storage_object.save(data, filename)


@click.command()
@click.pass_context
@cli_options.OPTION_CONFIG
@cli_options.OPTION_VERBOSITY
@click.option('--bbox', '-b', help='Bounding box filter')
@click.option('--download', '-d', is_flag=True, help='Download data')
def subscribe(ctx, config, download, bbox=[], verbosity='NOTSET'):
    """Subscribe to a broker/topic and optionally download data"""

    if config is None:
        raise click.ClickException('missing --config/-c')
    config = util.yaml_load(config)

    broker = config.get('broker')
    qos = int(config.get('qos', 1))
    topics = config.get('topics', [])
    options = {}

    if bbox:
        options['bbox'] = [float(i) for i in bbox.split(',')]

    if download:
        options['storage'] = config['storage']

    options['verify_data'] = config.get('verify_data', True)
    options['validate_message'] = config.get('validate_message', False)

    client = MQTTPubSubClient(broker, options)
    client.bind('on_message', on_message_handler)
    click.echo(f'Connected to broker {client.broker_safe_url}')
    click.echo(f'Subscribing to topics {topics}')
    client.sub(topics, qos)
