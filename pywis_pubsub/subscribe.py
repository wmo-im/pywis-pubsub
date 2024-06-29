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

import json
import logging
from pathlib import Path
import random

import click

from pywis_pubsub import cli_options
from pywis_pubsub import util
from pywis_pubsub.geometry import is_message_within_bbox
from pywis_pubsub.hook import load_hook
from pywis_pubsub.message import get_link, get_data
from pywis_pubsub.mqtt import MQTTPubSubClient
from pywis_pubsub.storage import STORAGES
from pywis_pubsub.validation import validate_message
from pywis_pubsub.verification import data_verified


LOGGER = logging.getLogger(__name__)


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
        if not bool(msg_dict['geometry']):
            LOGGER.error(f"Invalid geometry: {msg_dict['geometry']}")
            return
        if is_message_within_bbox(msg_dict['geometry'], userdata['bbox']):
            LOGGER.debug('Message geometry is within bbox')
        else:
            LOGGER.debug('Message geometry not within bbox; skipping')
            return

    clink = get_link(msg_dict['links'])
    if not clink:
        LOGGER.warning('No valid data link found')
        return

    LOGGER.info(f"Received message with data URL: {clink.get('href')}")

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

            LOGGER.debug(method)
            if not data_verified(data, size, method, value):
                LOGGER.error('Data verification failed; not saving')
                return
            else:
                LOGGER.debug('Data verification passed')

        filepath = userdata['storage']['options'].get('filepath', 'data_id')
        LOGGER.debug(f'Using {filepath} for naming filepath')

        link = get_link(msg_dict['links'])

        if filepath == 'link':
            LOGGER.debug('Using link as filepath')
            # fetch link and use local path, stripping slashes
            filename = link['href'].split('/', 3)[-1].strip('/')
        elif filepath == 'combined':
            LOGGER.debug('Using combined data_id+link extension as filepath')
            filename = msg_dict['properties']['data_id']
            suffix = Path(link).suffix
            if suffix != '':
                LOGGER.debug(f'File extension found: {suffix}')
                filename = f'{filename}{suffix}'
            else:
                LOGGER.debug('File extension not found. Trying media type')
                media_type = link.get('type')
                if media_type is not None:
                    suffix = util.guess_extension(media_type)
                    if suffix is not None:
                        filename = f'{filename}{suffix}'
                    else:
                        LOGGER.debug('No extension found. Giving up / using data_id')  # noqa
                else:
                    LOGGER.debug('No media type found. Giving up / using data_id')  # noqa
        else:
            LOGGER.debug('Using data_id as filepath')
            filename = msg_dict['properties'].get('data_id')
            if filename is None:
                LOGGER.error('no data_id found')
                return

        LOGGER.debug(f'filename: {filename}')

        content_type = link.get('type', 'applcation/octet-stream')

        storage_class = STORAGES[userdata.get('storage').get('type')]
        storage_object = storage_class(userdata['storage'])

        if not msg_dict['properties'].get('cache', True):
            LOGGER.debug(f'No caching requested; not saving {filename}')
            return

        if link.get('rel') == 'deletion':
            LOGGER.debug('Delete specified')
            storage_object.delete(filename)
        elif link.get('rel') == 'update':
            LOGGER.debug('Update specified')
            storage_object.save(data, filename, content_type)
        else:
            if storage_object.exists(filename):
                LOGGER.debug('Duplicate detected; not saving')
                return

            LOGGER.debug('Saving')
            storage_object.save(data, filename, content_type)

    if userdata.get('hook') is not None:
        LOGGER.debug(f"Hook detected: {userdata['hook']}")
        try:
            hook = load_hook(userdata['hook'])
            LOGGER.debug('Executing hook')
            hook.execute(msg.topic, msg_dict)
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
    client_id_prefix = config.get('client_id', 'pywis-pubsub')
    client_id = f'{client_id_prefix}-{random.randint(0, 1000)}'
    qos = int(config.get('qos', 1))
    subscribe_topics = config.get('subscribe_topics', [])
    verify_certs = config.get('verify_certs', True)
    certfile = config.get('certfile')
    keyfile = config.get('keyfile')

    options = {
        'verify_certs': verify_certs,
        'certfile': certfile,
        'keyfile': keyfile
    }
    options['client_id'] = client_id
    options['clean_session'] = config.get('clean_session', True)

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
