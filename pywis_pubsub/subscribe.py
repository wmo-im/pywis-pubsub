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
from copy import deepcopy
import json
import logging
from pathlib import Path
import random
from typing import Any, Callable
import urllib
from urllib.parse import urlparse

import click
from paho.mqtt import client as mqtt_client

from pywis_pubsub import cli_options
from pywis_pubsub.geometry import is_message_within_bbox
from pywis_pubsub import util

LOGGER = logging.getLogger(__name__)


class MQTTPubSubClient:
    """MQTT PubSub client"""
    def __init__(self, broker: str, options: dict = {}) -> None:
        """
        PubSub initializer

        :param config: RFC1738 URL of broker

        :returns: `None`
        """

        self.broker = broker
        self.broker_url = urlparse(self.broker)
        self.broker_safe_url = util.safe_url(self.broker)
        self.userdata = {}

        self.type = 'mqtt'
        self.port = self.broker_url.port
        self.client_id = f'pywis-pubsub-{random.randint(0, 1000)}'

        if options:
            self.userdata = deepcopy(options)

        msg = f'Connecting to broker {self.broker_safe_url} with id {self.client_id}'  # noqa
        LOGGER.debug(msg)
        self.conn = mqtt_client.Client(self.client_id, userdata=self.userdata)

        self.conn.enable_logger(logger=LOGGER)

        if None not in [self.broker_url.username, self.broker_url.password]:
            LOGGER.debug('Setting credentials')
            self.conn.username_pw_set(
                self.broker_url.username,
                self.broker_url.password)

        if self.port is None:
            if self.broker_url.scheme == 'mqtts':
                self.port = 8883
            else:
                self.port = 1883

        if self.broker_url.scheme == 'mqtts':
            self.conn.tls_set(tls_version=2)

        self.conn.connect(self.broker_url.hostname, self.port)
        self.bind('on_message', on_message_handler)
        LOGGER.debug('Connected to broker')

    def sub(self, topics: list, qos: int = 1) -> None:
        """
        Subscribe to a broker/topic(s)

        :param topic: `list` of topic(s)
        :param qos: `int` of quality of service (0, 1, 2)

        :returns: `None`
        """

        def on_connect(client, userdata, flags, rc):
            LOGGER.debug('Connected to broker {self.broker_safe_url}')
            LOGGER.debug('Subscribing to topics {topics}')
            for topic in topics:
                client.subscribe(topic, qos=qos)
                LOGGER.debug(f'Subscribed to topic {topic}, qos {qos}')

        def on_disconnect(client, userdata, rc):
            LOGGER.debug('Disconnected from {self.broker}')

        LOGGER.debug(f'Subscribing to broker {self.broker_safe_url}, topic(s) {topics}')  # noqa
        self.conn.on_connect = on_connect
        self.conn.on_disconnect = on_disconnect
        self.conn.loop_forever()

    def bind(self, event: str, function: Callable[..., Any]) -> None:
        """
        Binds an event to a function

        :param event: `str` of event name
        :param function: Python callable

        :returns: `None`
        """

        setattr(self.conn, event, function)

    def __repr__(self):
        return '<MQTTPubSubClient>'


def get_data(msg_json: dict) -> bytes:
    """
    Data downloading functionality

    :param msg_json: `dict` of notification message

    :returns: `bytes` of data
    """

    canonical_link = list(filter(lambda d: d['rel'] == 'canonical', msg_json['links']))  # noqa

    if canonical_link:
        LOGGER.debug('Found canonical link: {canonical_link}')

    if 'content' in msg_json and 'value' in msg_json['content']:
        LOGGER.debug('Decoding from inline data')
        data = base64.b64decode(msg_json['content']['value'])
    else:
        LOGGER.debug(f"Downloading from {canonical_link['href']}")
        with urllib.request.urlopen(canonical_link['href']) as f:
            data = f.read().decode('utf-8')

    return data


def on_message_handler(client, userdata, msg):
    """message handler"""

    msg_dict = json.loads(msg.payload)
    msg_json = json.dumps(msg_dict, indent=4)

    LOGGER.debug(f'Topic: {msg.topic}')
    LOGGER.debug(f'Raw message:\n{msg_json}')

    if userdata.get('bbox') and msg_dict.get('geometry') is not None:
        LOGGER.debug('Performing spatial filtering')
        if is_message_within_bbox(msg_dict['geometry'], userdata['bbox']):
            LOGGER.debug('Message geometry is within bbox')
        else:
            LOGGER.debug('Message geometry not within bbox; skipping')
            return

    if userdata.get('path') is not None:
        basepath = userdata['path'] / msg_dict['properties']['hierarchy']
        filename = basepath / msg_dict['properties']['instance_identifier']

        LOGGER.debug(f'Saving data to {filename}')
        data = get_data(msg_dict)

        LOGGER.debug(f'Creating directory {basepath}')
        Path(basepath).mkdir(parents=True, exist_ok=True)

        LOGGER.debug(f'Saving data to {filename}')
        with open(filename, 'wb') as fh:
            fh.write(data)


@click.command()
@click.pass_context
@cli_options.OPTION_CONFIG
@cli_options.OPTION_VERBOSITY
@click.option('--bbox', '-b', help='Bounding box filter')
@click.option('--download', '-d', is_flag=True, help='Download data')
def subscribe(ctx, config, download, bbox=[], verbosity='NOTSET'):
    """Subscribe to a broker/topic and optionally download data"""

    config = util.yaml_load(config)

    broker = config.get('broker')
    qos = int(config.get('qos', 1))
    topics = config.get('topics', [])
    options = {}

    if bbox:
        options['bbox'] = [float(i) for i in bbox.split(',')]

    if download:
        options['path'] = Path(config['storage'].get('path'))

    client = MQTTPubSubClient(broker, options)
    click.echo(f'Connected to broker {client.broker_safe_url}')

    click.echo(f'Subscribing to topics {topics}')
    client.sub(topics, qos)
