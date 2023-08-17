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
import json
import hashlib
import logging

import click

from pywis_pubsub import cli_options
from pywis_pubsub.message import get_canonical_link, get_data

LOGGER = logging.getLogger(__name__)


class VerificationMethods(enum.Enum):
    sha256 = 'sha256'
    sha384 = 'sha_384'
    sha512 = 'sha512'
    sha3_256 = 'sha3_256'
    sha3_384 = 'sha3_384'
    sha3_512 = 'sha3_512'


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
    data_value = hashlib.new(method)
    data_value.update(data)
    data_value_base64 = base64.b64encode(data_value.digest()).decode()

    LOGGER.debug('Comparing checksum and data size')

    return data_value_base64 == value and len(data) == size


def verify_data(instance: dict, verify_certs: bool = True) -> bool:
    """
    Validate a JSON instance document against an JSON schema

    :param instance: `dict` of JSON
    :param verify_certs: `bool` whether to verify server
                         certificates (default is `True`)

    :returns: `bool` of verification result
    """

    if 'integrity' not in instance['properties']:
        msg = 'No properties.integrity set. Exiting'
        LOGGER.error(msg)
        raise ValueError(msg)

    try:
        LOGGER.debug('Downloading data')
        data = get_data(instance, verify_certs)
    except Exception as err:
        LOGGER.error(err)
        raise ValueError(err)

    method = instance['properties']['integrity']['method']
    LOGGER.debug(f'method: {method}')
    value = instance['properties']['integrity']['value']
    LOGGER.debug(f'value: {value}')

    if 'content' in instance['properties']:
        size = instance['properties']['content']['size']
    else:
        size = get_canonical_link(instance['links'])['length']

    LOGGER.debug(f'size: {size}')
    return data_verified(data, size, method, value)


@click.command()
@click.pass_context
@click.argument('message', type=click.File())
@click.option('--verify-certs', '-vc', is_flag=True, default=True,
              help='Whether to verify server certificates')
@cli_options.OPTION_VERBOSITY
def verify(ctx, message, verify_certs, verbosity):
    """Verify a message"""

    try:
        message_dict = json.load(message)
    except json.decoder.JSONDecodeError as err:
        raise click.ClickException(f'Malformed message: {err}')

    verifies = verify_data(message_dict)

    if not verifies:
        raise click.ClickException('Verification failed')
    else:
        click.echo('Valid message')
