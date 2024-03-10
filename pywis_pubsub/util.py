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

from base64 import b64encode
from datetime import date, datetime, time, timezone
from decimal import Decimal
import logging
import mimetypes
import os
from pathlib import Path
import re
import ssl
from typing import Union
import yaml
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen

from requests import Session
from requests.adapters import HTTPAdapter, Retry

LOGGER = logging.getLogger(__name__)


def get_typed_value(value) -> Union[float, int, str]:
    """
    Derive true type from data value

    :param value: value

    :returns: value as a native Python data type
    """

    try:
        if '.' in value:  # float?
            value2 = float(value)
        elif len(value) > 1 and value.startswith('0'):
            value2 = value
        else:  # int?
            value2 = int(value)
    except ValueError:  # string (default)?
        value2 = value

    return value2


def json_serial(obj: object) -> Union[bytes, str, float]:
    """
    helper function to convert to JSON non-default
    types (source: https://stackoverflow.com/a/22238613)

    :param obj: `object` to be evaluated

    :returns: JSON non-default type to `str`
    """

    if isinstance(obj, (datetime, date, time)):
        LOGGER.debug('Returning as ISO 8601 string')
        return obj.isoformat().replace('+00:00', 'Z')
    elif isinstance(obj, bytes):
        try:
            LOGGER.debug('Returning as UTF-8 decoded bytes')
            return obj.decode('utf-8')
        except UnicodeDecodeError:
            LOGGER.debug('Returning as base64 encoded JSON object')
            return b64encode(obj)
    elif isinstance(obj, Decimal):
        LOGGER.debug('Returning as float')
        return float(obj)
    elif isinstance(obj, Path):
        LOGGER.debug('Returning as path string')
        return str(obj)

    msg = f'{obj} type {type(obj)} not serializable'
    LOGGER.error(msg)
    raise TypeError(msg)


def yaml_load(fh) -> dict:
    """
    serializes a YAML files into a pyyaml object

    :param fh: file handle

    :returns: `dict` representation of YAML
    """

    # support environment variables in config
    # https://stackoverflow.com/a/55301129
    path_matcher = re.compile(r'.*\$\{([^}^{]+)\}.*')

    def path_constructor(loader, node):
        env_var = path_matcher.match(node.value).group(1)
        if env_var not in os.environ:
            msg = f'Undefined environment variable {env_var} in config'
            raise EnvironmentError(msg)
        return get_typed_value(os.path.expandvars(node.value))

    class EnvVarLoader(yaml.SafeLoader):
        pass

    EnvVarLoader.add_implicit_resolver('!path', path_matcher, None)
    EnvVarLoader.add_constructor('!path', path_constructor)

    return yaml.load(fh, Loader=EnvVarLoader)


def yaml_dump(fh: str, content: dict) -> None:
    """
    Writes serialized YAML to file

    :param fh: file handle
    :param content: dict, yaml file content

    :returns: `None`
    """

    return yaml.safe_dump(content, fh, sort_keys=False, indent=4)


def safe_url(url):
    """
    Returns a safe RFC1738 removing embedded authentication/credentials

    :param url: RFC1738 URL

    :returns: URL stripped of authentication/credentials
    """

    u = urlparse(url)

    safe_url = f'{u.scheme}://{u.hostname}'
    if u.port is not None:
        safe_url = f'{safe_url}:{u.port}'

    return safe_url


def get_userdir() -> str:
    """
    Helper function to get userdir

    :returns: user's home directory
    """

    return Path.home() / '.pywis-pubsub'


def get_http_session():
    """
    Get HTTP session

    :returns: `requests.Session`
    """

    s = Session()
    retries = Retry(
        connect=1,
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=2
    )

    adapter = HTTPAdapter(max_retries=retries)
    s.mount('https://', adapter)
    s.mount('http://', adapter)

    return s


def guess_extension(media_type: str) -> str:
    """
    Guess file extension from known WMO media types:

    :media_type: `str` of media type

    :returns: `str` of file extension
    """

    extension = None

    wmo_extra_types = {
        'application/x-bufr': '.bufr4',
        'application/x-grib': '.grib2',
        'application/cap+xml': '.cap'
    }

    for key, value in wmo_extra_types.items():
        mimetypes.add_type(key, value)

    extension = mimetypes.guess_extension(media_type)
    LOGGER.debug(f'Found {extension}')

    return extension


def get_cli_common_options(function):
    """
    Define common CLI options
    """

    import click
    function = click.option('--verbosity', '-v',
                            type=click.Choice(
                                ['ERROR', 'WARNING', 'INFO', 'DEBUG']),
                            help='Verbosity')(function)
    function = click.option('--log', '-l', 'logfile',
                            type=click.Path(writable=True, dir_okay=False),
                            help='Log file')(function)
    return function


def urlopen_(url: str):
    """
    Helper function for downloading a URL

    :param url: URL to download

    :returns: `http.client.HTTPResponse`
    """

    try:
        response = urlopen(url)
    except (ssl.SSLError, URLError) as err:
        LOGGER.warning(err)
        LOGGER.warning('Creating unverified context')
        context = ssl._create_unverified_context()

        response = urlopen(url, context=context)

    return response


def get_current_datetime_rfc3339() -> str:
    """
    Gets the current datetime in RFC3339 format

    :returns: `str` of RFC3339 datetime
    """

    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
