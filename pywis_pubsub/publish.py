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

from datetime import datetime
from enum import Enum
import hashlib
import json
import logging
import mimetypes

import click
import requests

from pywis_pubsub import cli_options
from pywis_pubsub import util
from pywis_pubsub.mqtt import MQTTPubSubClient
from pywis_pubsub.validation import validate_


LOGGER = logging.getLogger(__name__)


class SecureHashAlgorithms(Enum):
    sha256 = 'sha256'
    sha384 = 'sha_384'
    sha512 = 'sha512'
    sha3_256 = 'sha3_256'
    sha3_384 = 'sha3_384'
    sha3_512 = 'sha3_512'


def generate_checksum(data: bytes, algorithm: SecureHashAlgorithms) -> str:
    """
    Generate a checksum of message file

    :param data: bytes of data
    :param algorithm: secure hash algorithm (SecureHashAlgorithm)

    :returns: hexdigest
    """

    sh = getattr(hashlib, algorithm)()
    sh.update(data)

    hexdigest = sh.hexdigest()
    LOGGER.debug(f'Hex digest: {hexdigest}')
    return hexdigest


def get_file_info(public_data_url: str) -> dict:
    """
    get filename, length and calculate checksum from public URL

    :param public_data_url: `str` defining publicly accessible URL

    :returns: `dict` of file information
    """

    res = requests.get(public_data_url)
    # raise HTTPError, if on occurred:
    res.raise_for_status()

    filebytes = res.content
    checksum_type = SecureHashAlgorithms.sha512.value
    return {
        'filename': public_data_url.split('/')[-1],
        'checksum_value': generate_checksum(filebytes, checksum_type),
        'checksum_type': checksum_type,
        'size': len(filebytes)
    }


def create_message(topic: str, content_type: str, url: str, identifier: str,
                   geometry = [], wigos_station_identifier: str = None) -> dict:  # noqa
    """
    Create WIS2 compliant message

    :param topic: `str` of topic
    :url: `str` of url pointing to data
    :identifier: `str` of unique-id to help global broker deduplicate data
    :geometry: point array defining longitude,latitude,elevation
               (elevation is optional
    :wigos_station_identifier: `str` of WSI for station as used in OSCAR

    :returns: `dict` of message
    """

    publish_datetime = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    # get filename, length and calculate checksum
    # raises HTTPError if file can not be accessed
    file_info = get_file_info(url)

    if geometry:
        point = [float(i) for i in geometry.split(',')]
        geometry2 = {
            'type': 'Point',
            'coordinates': point
        }
    else:
        geometry2 = None

    if content_type is None:
        content_type2 = mimetypes.guess_type(url)[0]
        if content_type2 is None:
            LOGGER.warning('Unknown content type')
            content_type2 = 'application/octet-stream'
    else:
        content_type2 = content_type

    message = {
            'id': identifier,
            'type': 'Feature',
            'version': 'v04',
            'geometry': geometry2,
            'properties': {
                'data_id': f"{topic}/{file_info['filename']}",
                'pubtime': publish_datetime,
                'integrity': {
                    'method': file_info['checksum_type'],
                    'value': file_info['checksum_value']
                },
            },
            'links': [{
                'rel': 'canonical',
                'type': content_type2,
                'href': url,
                'length': file_info['size']
            }]
    }
    if wigos_station_identifier is not None:
        message['properties']['wigos_station_identifier'] = wigos_station_identifier  # noqa

    return message


@click.command()
@click.pass_context
@cli_options.OPTION_CONFIG
@cli_options.OPTION_VERBOSITY
@click.option('--file', '-f', 'file_', type=click.File(), help='url of data')
@click.option('--url', '-u', help='url of data')
@click.option('--identifier', '-i', help='unique file identifier')
@click.option('--topic', '-t', help='topic to publish to')
@click.option('--geometry', '-g',
              help='point geometry as longitude,latitude,elevation (elevation is optional)')  # noqa
@click.option('--wigos_station_identifier', '-w',
              help='WIGOS station identifier')
def publish(ctx, file_, config, url, topic, identifier, geometry=[],
            wigos_station_identifier=None, verbosity='NOTSET'):
    """Publish a WIS2 Notification Message"""

    if config is None:
        raise click.ClickException('missing -c/--config')

    if file_ is None and None in [url, identifier]:
        raise click.ClickException('missing required arguments')

    config = util.yaml_load(config)

    broker = config.get('broker')

    if topic is None:
        topic2 = config.get('publish_topic')
    else:
        topic2 = topic

    if file_ is not None:
        if config.get('validate_message', False):
            ctx.invoke(validate_, message=file_)
            file_.seek(0)
        message = json.load(file_)
    else:
        message = create_message(
            topic=topic2,
            content_type=config.get('content_type'),
            url=url,
            identifier=identifier,
            geometry=geometry,
            wigos_station_identifier=wigos_station_identifier
        )

    client = MQTTPubSubClient(broker)
    click.echo(f'Connected to broker {client.broker_safe_url}')
    click.echo(f'Publishing message to topic={topic2}')
    client.pub(topic2, json.dumps(message))
