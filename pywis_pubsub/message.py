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
import logging

import click

from pywis_pubsub.util import get_http_session


LOGGER = logging.getLogger(__name__)


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
            http_session.close()
        except Exception as err:
            LOGGER.error(f"download error ({canonical_link['href']}): {err}")
            raise

    return data


@click.group()
def message():
    """Message utilities"""
    pass
