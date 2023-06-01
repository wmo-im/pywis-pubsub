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
from typing import Tuple

import click
from jsonschema import validate

from pywis_pubsub import cli_options
from pywis_pubsub.schema import MESSAGE_SCHEMA

LOGGER = logging.getLogger(__name__)


@click.group()
def message():
    """Message utilities"""
    pass


def validate_message(instance: dict) -> Tuple[bool, str]:
    """
    Validate a JSON instance document against an JSON schema

    :param instance: `dict` of JSON

    :returns: `tuple` of `bool` of validation result
              and `str` of error message(s)
    """

    success = False
    error_message = None

    if not MESSAGE_SCHEMA.exists():
        msg = 'Schema not found. Please run pywis-pubsub schema sync'
        LOGGER.error(msg)
        raise RuntimeError(msg)

    with open(MESSAGE_SCHEMA) as fh:
        schema = json.load(fh)

    try:
        validate(instance, schema)
        success = True
    except Exception as err:
        error_message = repr(err)

    return (success, error_message)


@click.command('validate')
@click.pass_context
@click.argument('message', type=click.File())
@cli_options.OPTION_VERBOSITY
def validate_(ctx, message, verbosity):
    """Validate a message"""

    try:
        message_dict = json.load(message)
    except json.decoder.JSONDecodeError as err:
        raise click.ClickException(f'Malformed message: {err}')

    is_valid, errors = validate_message(message_dict)

    if not is_valid:
        raise click.ClickException(f'Invalid message: {errors}')
    else:
        click.echo('Valid message')


message.add_command(validate_)
