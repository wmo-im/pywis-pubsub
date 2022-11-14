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

import click
import logging
from pathlib import Path
from urllib.request import urlopen

from pywis_pubsub import cli_options

LOGGER = logging.getLogger(__name__)

MESSAGE_SCHEMA_URL = 'https://raw.githubusercontent.com/wmo-im/wis2-notification-message/main/WIS2_Message_Format_Schema.yaml'  # noqa
USERDIR = Path.home() / '.pywis-pubsub'
MESSAGE_SCHEMA = USERDIR / 'wis2-notification-message' / 'WIS2_Message_Format_Schema.yaml'  # noqa


def cache_schema() -> None:
    """
    Cache WIS2 notification schema

    :returns: `None`
    """

    LOGGER.debug('Caching notification message schema')

    if not MESSAGE_SCHEMA.parent.exists():
        LOGGER.debug(f'Creating cache directory {MESSAGE_SCHEMA.parent}')
        MESSAGE_SCHEMA.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.debug('Downloading message schema')
    with MESSAGE_SCHEMA.open('wb') as fh:
        fh.write(urlopen(MESSAGE_SCHEMA_URL).read())


@click.group()
def schema():
    """Notification schema management"""
    pass


@click.command()
@click.pass_context
@cli_options.OPTION_VERBOSITY
def cache(ctx, verbosity):
    """Cache WIS2 notification schema"""

    click.echo('Caching notification message schema')
    cache_schema()


schema.add_command(cache)
