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

# WIS2 Notification Message Key Performance Indicators (KPIs)

import json
import logging
import os

import click
import requests

import pywis_pubsub
from pywis_pubsub.ets import WNMTestSuite
from pywis_pubsub .util import (get_cli_common_options,
                                get_current_datetime_rfc3339, urlopen_)

LOGGER = logging.getLogger(__name__)

ROUND = 3


class WNMKeyPerformanceIndicators:
    """Key Performance Indicators for WIS2 Notification Message"""

    def __init__(self, data):
        """
        initializer

        :param data: dict of WNM JSON

        :returns: `pywis_pubsub.kpi.WNMKeyPerformanceIndicators`
        """

        self.data = data
        self.codelists = None

    @property
    def identifier(self):
        """
        Helper function to derive a notification identifier

        :returns: notification identifier
        """

        return self.data['id']

    def kpi_metadata_id(self) -> tuple:
        """
        Implements KPI for Metadata identifier

        :returns: `tuple` of KPI name, achieved score, total score,
                  and comments
        """

        total = 2
        score = 0
        comments = []

        name = 'KPI: Metadata identifier'

        LOGGER.info(f'Running {name}')

        if 'metadata_id' in self.data['properties']:
            score += 1

            metadata_id = self.data['properties']['metadata_id']

            gdc_url = os.environ.get('PYWIS_PUBSUB_GDC_URL')

            if gdc_url is None:
                msg = 'PYWIS_PUBSUB_GDC_URL environment variable not set'
                raise RuntimeError(msg)

            url = f'{gdc_url}/collections/wis2-discovery-metadata/items/{metadata_id}'  # noqa

            LOGGER.debug(f'Checking for discovery metadata at {url}')
            response = requests.get(url)
            try:
                response.raise_for_status()
                score += 1
            except requests.exceptions.HTTPError:
                comments.append(f'Discovery metadata not found at {url}')
        else:
            comments.append('No metadata identifier found in notification')

        return name, total, score, comments

    def evaluate(self, kpi: str = None) -> dict:
        """
        Convenience function to run all tests

        :param kpi: `str` of KPI identifier

        :returns: `dict` of overall test report
        """

        kpis_to_run = []

        for f in dir(WNMKeyPerformanceIndicators):
            if all([
                    callable(getattr(WNMKeyPerformanceIndicators, f)),
                    f.startswith('kpi_')]):

                kpis_to_run.append(f)

        if kpi is not None:
            selected_kpi = f'kpi_{kpi}'
            if selected_kpi not in kpis_to_run:
                msg = f'Invalid KPI number: {selected_kpi} is not in {kpis_to_run}'  # noqa
                LOGGER.error(msg)
                raise ValueError(msg)
            else:
                kpis_to_run = [selected_kpi]

        LOGGER.info(f'Evaluating KPIs: {kpis_to_run}')

        results = {}

        for kpi in kpis_to_run:
            LOGGER.debug(f'Running {kpi}')
            result = getattr(self, kpi)()
            LOGGER.debug(f'Raw result: {result}')
            LOGGER.debug('Calculating result')
            try:
                percentage = round(float((result[2] / result[1]) * 100), ROUND)
            except ZeroDivisionError:
                percentage = None

            results[kpi] = {
                'name': result[0],
                'total': result[1],
                'score': result[2],
                'comments': result[3],
                'percentage': percentage
            }
            LOGGER.debug(f'{kpi}: {result[1]} / {result[2]} = {percentage}')

        LOGGER.debug('Calculating total results')
        results['summary'] = generate_summary(results)
        results['summary']['identifier'] = self.identifier
        overall_grade = 'F'
        overall_grade = calculate_grade(results['summary']['percentage'])
        results['summary']['grade'] = overall_grade

        results['datetime'] = get_current_datetime_rfc3339()
        results['generated-by'] = f'pywis-pubsub {pywis_pubsub.__version__} (https://github.com/wmo-im/pywis-pubsub)'  # noqa

        return results


def generate_summary(results: dict) -> dict:
    """
    Generates a summary entry for given group of results

    :param results: results to generate the summary from

    :returns: `dict` of summary report
    """

    sum_total = sum(v['total'] for v in results.values())
    sum_score = sum(v['score'] for v in results.values())
    comments = {k: v['comments'] for k, v in results.items() if v['comments']}

    try:
        sum_percentage = round(float((sum_score / sum_total) * 100), ROUND)
    except ZeroDivisionError:
        sum_percentage = None

    summary = {
        'total': sum_total,
        'score': sum_score,
        'comments': comments,
        'percentage': sum_percentage,
    }

    return summary


def calculate_grade(percentage: float) -> str:
    """
    Calculates letter grade from numerical score

    :param percentage: float between 0-100

    :returns: `str` of calculated letter grade
    """

    if percentage is None:
        grade = None
    elif percentage > 100 or percentage < 0:
        raise ValueError('Invalid percentage')
    elif percentage >= 80:
        grade = 'A'
    elif percentage >= 65:
        grade = 'B'
    elif percentage >= 50:
        grade = 'C'
    elif percentage >= 35:
        grade = 'D'
    elif percentage >= 20:
        grade = 'E'
    else:
        grade = percentage

    return grade


@click.group()
def kpi():
    """key performance indicators"""
    pass


@click.command()
@click.pass_context
@get_cli_common_options
@click.argument('file_or_url')
@click.option('--fail-on-ets/--no-fail-on-ets',
              '-f', default=True, help='Stop the KPI on failing ETS')
@click.option('--summary', '-s', is_flag=True, default=False,
              help='Provide summary of KPI test results')
@click.option('--kpi', '-k', help='KPI to run, default is all')
def validate(ctx, file_or_url, logfile, verbosity, kpi, summary,
             fail_on_ets=True):
    """run key performance indicators"""

    click.echo(f'Opening {file_or_url}')

    if file_or_url.startswith('http'):
        content = urlopen_(file_or_url).read()
    else:
        with open(file_or_url) as fh:
            content = fh.read()

    content = json.loads(content)
    click.echo(f'Validating {file_or_url}')

    if fail_on_ets:
        ts = WNMTestSuite(content)
        try:
            _ = ts.run_tests(fail_on_ets)
        except Exception as err:
            raise click.ClickException(err)
            ctx.exit(1)

    kpis = WNMKeyPerformanceIndicators(content)

    try:
        kpis_results = kpis.evaluate(kpi)
    except ValueError as err:
        raise click.UsageError(f'Invalid KPI {kpi}: {err}')
        ctx.exit(1)

    if not summary or kpi is not None:
        click.echo(json.dumps(kpis_results, indent=4))
    else:
        click.echo(json.dumps(kpis_results['summary'], indent=4))


kpi.add_command(validate)
