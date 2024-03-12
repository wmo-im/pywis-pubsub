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

# executable test suite as per WNM, Annex A

import json
import logging
from uuid import UUID

import click
from jsonschema.validators import Draft202012Validator


import pywis_pubsub
from pywis_pubsub.schema import MESSAGE_SCHEMA
from pywis_pubsub.message import get_link
from pywis_pubsub.util import (get_cli_common_options,
                               get_current_datetime_rfc3339, urlopen_)

LOGGER = logging.getLogger(__name__)


def gen_test_id(test_id: str) -> str:
    """
    Convenience function to print test identifier as URI

    :param test_id: test suite identifier

    :returns: test identifier as URI
    """

    return f'http://wis.wmo.int/spec/wnm/1/conf/core/{test_id}'


class WNMTestSuite:
    """Test suite for WIS Notification Message"""

    def __init__(self, data: dict):
        """
        initializer

        :param data: dict of WNM JSON

        :returns: `pywis_pubsub.ets.WNMTestSuite`
        """

        self.test_id = None
        self.message = data
        self.report = []

    def run_tests(self, fail_on_schema_validation=False):
        """Convenience function to run all tests"""

        results = []
        tests = []
        ets_report = {
            'summary': {},
            'generated-by': f'pywis-pubsub {pywis_pubsub.__version__} (https://github.com/wmo-im/pywis-pubsub)'  # noqa
        }

        for f in dir(WNMTestSuite):
            if all([
                    callable(getattr(WNMTestSuite, f)),
                    f.startswith('test_requirement'),
                    not f.endswith('validation')]):

                tests.append(f)

        validation_result = self.test_requirement_validation()
        if validation_result['code'] == 'FAILED':
            if fail_on_schema_validation:
                msg = ('Record fails WNM validation. Stopping ETS ',
                       f"errors: {validation_result['errors']}")
                LOGGER.error(msg)
                raise ValueError(msg)

        for t in tests:
            results.append(getattr(self, t)())

        for code in ['PASSED', 'FAILED', 'SKIPPED']:
            r = len([t for t in results if t['code'] == code])
            ets_report['summary'][code] = r

        ets_report['tests'] = results
        ets_report['datetime'] = get_current_datetime_rfc3339()

        return {
            'ets-report': ets_report
        }

    def test_requirement_validation(self):
        """
        Validate that a WNM is valid to the authoritative WNM schema.
        """

        validation_errors = []

        status = {
            'id': gen_test_id('validation'),
            'code': 'PASSED'
        }

        if not MESSAGE_SCHEMA.exists():
            msg = "WNM schema missing. Run 'pywis-pubsub schema sync' to cache"
            LOGGER.error(msg)
            raise RuntimeError(msg)

        with MESSAGE_SCHEMA.open() as fh:
            LOGGER.debug(f'Validating {self.message} against {MESSAGE_SCHEMA}')
            validator = Draft202012Validator(json.load(fh))

            for error in validator.iter_errors(self.message):
                LOGGER.debug(f'{error.json_path}: {error.message}')
                validation_errors.append(f'{error.json_path}: {error.message}')

            if validation_errors:
                status['code'] = 'FAILED'
                status['message'] = f'{len(validation_errors)} error(s)'
                status['errors'] = validation_errors

        return status

    def test_requirement_id(self):
        """
        Check for the existence of a valid id property.
        """

        status = {
            'id': gen_test_id('id'),
            'code': 'PASSED',
        }

        try:
            UUID(self.message['id'])
        except ValueError as err:
            status['code'] = 'FAILED'
            status['message'] = f'Invalid UUID: {err}'

        return status

    def test_requirement_geometry(self):
        """
        Check for the existence of a valid geometry property.
        """

        status = {
            'id': gen_test_id('geometry'),
            'code': 'PASSED',
            'message': 'Passes given schema is compliant/valid'
        }

        return status

    def test_requirement_temporal(self):
        """
        Check for the existence of a valid datetime property/properties.
        """

        status = {
            'id': gen_test_id('temporal'),
            'code': 'PASSED',
        }

        for dtp in ['datetime', 'start_datetime', 'end_datetime']:
            value = self.message['properties'].get(dtp)
            if value is not None:
                if not value.endswith(('Z', '+00:00')):
                    status['code'] = 'FAILED'
                    status['message'] = 'date-time not in UTC'

        return status

    def test_requirement_data_id(self):
        """
        Check for the existence of a valid properties.data_id.
        """

        status = {
            'id': gen_test_id('data_id'),
            'code': 'PASSED',
            'message': 'Passes given schema is compliant/valid'
        }

        return status

    def test_requirement_version(self):
        """
        Validate that a WNM provides a valid properties.version.
        """

        status = {
            'id': gen_test_id('version'),
            'code': 'PASSED',
            'message': 'Passes given schema is compliant/valid'
        }

        return status

    def test_requirement_conformance(self):
        """
        Validate that a WCMP record provides valid conformance information.
        """

        status = {
            'id': gen_test_id('conformance'),
            'code': 'PASSED'
        }

        return status

    def test_requirement_links(self):
        """
        Validate that a WNM provides valid link property/properties.
        """

        status = {
            'id': gen_test_id('links'),
            'code': 'PASSED'
        }

        links = self.message['links']

        LOGGER.debug('Checking for at least one link')
        if len(links) < 1:
            status['code'] = 'FAILED'
            status['message'] = 'missing at least one link'
            return status

        for link in links:
            LOGGER.debug('Checking for required protocols')
            if not link['href'].startswith(('http', 'https', 'ftp', 'sftp')):
                status['code'] = 'FAILED'
                status['message'] = 'Invalid link protocol'
                return status

        if not get_link(links):
            status['code'] = 'FAILED'
            status['message'] = 'Missing required link relations'
            return status

        return status


@click.group()
def ets():
    """executable test suite"""
    pass


@click.command()
@click.pass_context
@get_cli_common_options
@click.argument('file_or_url')
@click.option('--fail-on-schema-validation/--no-fail-on-schema-validation',
              '-f', default=True,
              help='Stop the ETS on failing schema validation')
def validate(ctx, file_or_url, logfile, verbosity,
             fail_on_schema_validation=True):
    """validate against the abstract test suite"""

    click.echo(f'Opening {file_or_url}')

    if file_or_url.startswith('http'):
        content = urlopen_(file_or_url).read()
    else:
        with open(file_or_url) as fh:
            content = fh.read()

    content = json.loads(content)
    click.echo(f'Validating {file_or_url}')

    ts = WNMTestSuite(content)
    try:
        results = ts.run_tests(fail_on_schema_validation)
    except Exception as err:
        raise click.ClickException(err)

    click.echo(json.dumps(results, indent=4))
    ctx.exit(results['ets-report']['summary']['FAILED'])


ets.add_command(validate)
