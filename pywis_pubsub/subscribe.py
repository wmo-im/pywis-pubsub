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

import click
from requests import Session
from requests.adapters import HTTPAdapter, Retry

from pywis_pubsub import cli_options
from pywis_pubsub import util
from pywis_pubsub.geometry import is_message_within_bbox
from pywis_pubsub.hook import load_hook
from pywis_pubsub.mqtt import MQTTPubSubClient
from pywis_pubsub.storage import STORAGES
from pywis_pubsub.validation import validate_message


LOGGER = logging.getLogger(__name__)


class VerificationMethods(enum.Enum):
    sha256 = 'sha256'
    sha384 = 'sha_384'
    sha512 = 'sha512'
    sha3_256 = 'sha3_256'
    sha3_384 = 'sha3_384'
    sha3_512 = 'sha3_512'


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


def get_data(msg_dict: dict, verify_certs=True) -> bytes:
    """
    Data downloading functionality

    :param msg_dict: `dict` of notification message
    :param verify_certs: `bool` of whether to verify
                         certificates (default true)

    :returns: `bytes` of data
    """

    canonical_link = get_canonical_link(msg_dict['links'])

    if canonical_link:
        LOGGER.debug(f'Found canonical link: {canonical_link}')

    if 'content' in msg_dict and 'value' in msg_dict['content']:
        LOGGER.debug('Decoding from inline data')
        data = base64.b64decode(msg_dict['content']['value'])
    else:
        LOGGER.debug(f"Downloading from {canonical_link['href']}")
        LOGGER.debug(f'Certificate verification: {verify_certs}')
        http_session = get_http_session()
        try:
            data = http_session.get(canonical_link['href'],
                                    verify=verify_certs).content
        except Exception as err:
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


def get_http_session():
    """
    Get HTTP session

    :returns: `requests.Session`
    """

    s = Session()
    retries = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=2
    )

    adapter = HTTPAdapter(max_retries=retries)
    s.mount('https://', adapter)
    s.mount('http://', adapter)

    return s


def on_message_handler(client, userdata, msg):
    """message handler"""

    LOGGER.debug(f'Topic: {msg.topic}')
    LOGGER.debug(f'Message:\n{msg.payload}')

    msg_dict = json.loads(msg.payload)

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
            data = get_data(msg_dict, userdata.get('verify_certs'))
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

    if userdata.get('hook') is not None:
        LOGGER.debug(f"Hook detected: {userdata['hook']}")
        try:
            hook = load_hook(userdata['hook'])
            LOGGER.debug('Executing hook')
            hook.execute(msg_dict)
        except Exception as err:
            msg = f'Hook failed: {err}'
            LOGGER.error(msg, exc_info=True)


@click.command()
@click.pass_context
@cli_options.OPTION_CONFIG
@cli_options.OPTION_VERBOSITY
@click.option('--bbox', '-b', help='Bounding box filter')
@click.option('--download', '-d', is_flag=True, help='Download data')
def subscribe(ctx, config, download, bbox=[], verbosity='NOTSET'):
    """Subscribe to a broker/topic and optionally download data"""

    if config is None:
        raise click.ClickException('missing --config')
    config = util.yaml_load(config)

    broker = config.get('broker')
    qos = int(config.get('qos', 1))
    subscribe_topics = config.get('subscribe_topics', [])
    verify_certs = config.get('verify_certs', True)

    options = {
        'verify_certs': verify_certs
    }

    if bbox:
        options['bbox'] = [float(i) for i in bbox.split(',')]

    if download:
        options['storage'] = config['storage']

    options['verify_data'] = config.get('verify_data', True)
    options['validate_message'] = config.get('validate_message', False)
    options['hook'] = config.get('hook')

    client = MQTTPubSubClient(broker, options)
    client.bind('on_message', on_message_handler)
    click.echo(f'Connected to broker {client.broker_safe_url}')
    click.echo(f'Subscribing to subscribe_topics {subscribe_topics}')
    client.sub(subscribe_topics, qos)
