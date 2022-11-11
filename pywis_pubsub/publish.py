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

import hashlib
import json
import logging

import click
import requests

from pywis_pubsub import cli_options
from pywis_pubsub import util
from pywis_pubsub.mqtt import MQTTPubSubClient

from datetime import datetime
from enum import Enum

LOGGER = logging.getLogger(__name__)


class SecureHashAlgorithms(Enum):
    SHA512 = 'sha512'
    MD5 = 'md5'


MIMETYPES = [
    'text/plain',
    'text/csv',
    'application/octet-stream',
    'application/text',
    'application/json',
    'application/x-bufr',
    'application/x-grib2'
    ]


def generate_checksum(bytes, algorithm: SecureHashAlgorithms) -> str:  # noqa
    """
    Generate a checksum of message file

    :param algorithm: secure hash algorithm (md5, sha512)

    :returns: hexdigest
    """

    sh = getattr(hashlib, algorithm)()
    sh.update(bytes)
    return sh.hexdigest()


def get_file_info(public_data_url):
    """ get filename, length and calculate checksum from public-file-url """
    res = requests.get(public_data_url)
    # raise HTTPError, if on occurred:
    res.raise_for_status()
    filebytes = res.content
    checksum_type = SecureHashAlgorithms.SHA512.value
    return {
        'filename': public_data_url.split('/')[-1],
        'checksum_value': generate_checksum(filebytes, checksum_type),
        'checksum_type': checksum_type,
        'size': len(filebytes)
    }


@click.command()
@click.pass_context
@cli_options.OPTION_CONFIG
@cli_options.OPTION_VERBOSITY
@click.option('--url', '-u', help='url pointing to data-file')
@click.option('--geometry', '-g', help='geometry as lat,lon for example -g 34.07,-14.4 ') # noqa
@click.option('--wigos_id', '-w', help='optional wigos-id')
def publish(ctx, config, url, geometry=[], wigos_id=None, verbosity='NOTSET'):
    """ Publish a WIS2-message for a given url and a set of coordinates """

    if config is None:
        raise click.ClickException('missing --config/-c')
    config = util.yaml_load(config)

    broker = config.get('broker')
    topic = config.get('topic', [])
    application_type = config.get('application_type', [])
    if application_type not in MIMETYPES:
        click.echo(f"application_type={application_type} is invalid")
        click.echo(f"options are: {MIMETYPES}")
        return
    file_encoding = 'utf-8'
    if application_type in ['application/x-bufr', 'application/x-grib2']:
        file_encoding = 'base64'

    publish_datetime = datetime.utcnow().strftime(
            '%Y-%m-%dT%H:%M:%SZ'
    )
    latlon = [float(i) for i in geometry.split(',')]
    geometry = {
        "type": "Point",
        "coordinates": latlon
    }
    # get filename, length and calculate checksum
    # raises exception if file can not be accessed
    file_info = get_file_info(url)

    message = {
            'id': file_info['filename'],
            'type': 'Feature',
            'version': 'v04',
            'geometry': geometry,
            'properties': {
                'data_id': f"{topic}/{file_info['filename']}",
                'pubtime': publish_datetime,
                'integrity': {
                    'method': file_info['checksum_type'],
                    'value': file_info['checksum_value']
                },
                'content': {
                    'size': file_info['size'],
                    'encoding': file_encoding
                },
            },
            'links': [{
                'rel': 'canonical',
                'type': application_type,
                'href': url
            }]
    }
    if wigos_id is not None:
        message['properties']['wigos_station_identifier'] = wigos_id  # noqa

    client = MQTTPubSubClient(broker)
    click.echo(f'Connected to broker {client.broker_safe_url}')
    click.echo(f'Publish new message to topic={topic}')
    client.pub(topic, json.dumps(message))
