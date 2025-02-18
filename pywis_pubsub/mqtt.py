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

from copy import deepcopy
import logging
import random
import ssl
from typing import Any, Callable
from urllib.parse import urlparse

from paho.mqtt import client as mqtt_client
from pywis_pubsub import util

LOGGER = logging.getLogger(__name__)


class MQTTPubSubClient:
    """MQTT PubSub client"""
    def __init__(self, broker: str, options: dict = {}, conn=None) -> None:
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
            client_id_prefix = options.get('client_id', 'pywis-pubsub')
            self.client_id = f'{client_id_prefix}-{random.randint(0, 1000)}'

        transport = 'tcp'
        # if scheme is ws or wss, set transport to websockets
        if self.broker_url.scheme in ['ws', 'wss']:
            transport = 'websockets'

        if conn is not None:
            LOGGER.debug(f'Using existing connection {conn}')
            self.conn = conn
            return

        msg = f'Connecting to broker {self.broker_safe_url} with id {self.client_id}'  # noqa
        LOGGER.debug(msg)
        self.conn = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2,
                                       self.client_id,
                                       userdata=self.userdata,
                                       transport=transport)

        self.conn.enable_logger(logger=LOGGER)

        if self.broker_url.scheme in ['ws', 'wss']:
            LOGGER.debug('Setting Websockets path: {self.broker_url.path}')
            self.conn.ws_set_options(self.broker_url.path)

        if None not in [self.broker_url.username, self.broker_url.password]:
            LOGGER.debug('Setting credentials')
            self.conn.username_pw_set(
                self.broker_url.username,
                self.broker_url.password)

        # guess the port if not specified from the scheme
        if self.port is None:
            if self.broker_url.scheme == 'mqtts':
                self.port = 8883
            elif self.broker_url.scheme == 'wss':
                self.port = 443
            elif self.broker_url.scheme == 'ws':
                self.port = 80
            else:
                self.port = 1883

        if self.port in [443, 8883]:
            tls_settings = {
                'tls_version': 2
            }

            if not options.get('verify_certs'):
                tls_settings['cert_reqs'] = ssl.CERT_NONE

            LOGGER.debug(f'TLS settings: {tls_settings}')
            if options.get('certfile') is not None:
                LOGGER.debug('Setting TLS certfile')
                self.conn.tls_set(options.get('certfile'), **tls_settings)
            elif options.get('keyfile') is not None:
                LOGGER.debug('Setting TLS keyfile')
                self.conn.tls_set(options.get('keyfile'), **tls_settings)
            else:
                LOGGER.debug('Setting TLS defaults')
                self.conn.tls_set(**tls_settings)

        self.conn.connect(self.broker_url.hostname, self.port)
        LOGGER.debug('Connected to broker')

    def pub(self, topic: str, message: str, qos: int = 1) -> bool:
        """
        Publish a message to a broker/topic

        :param topic: `str` of topic
        :param message: `str` of message

        :returns: `bool` of publish result
        """

        self.conn.loop()
        LOGGER.debug(f'Publishing to broker {self.broker_safe_url}')
        LOGGER.debug(f'Topic: {topic}')
        LOGGER.debug(f'Message: {message}')

        result = self.conn.publish(topic, message, qos)

        # TODO: investigate implication
        # result.wait_for_publish()

        if result.is_published:
            return True
        else:
            msg = f'Publishing error code: {result[1]}'
            LOGGER.warning(msg)
            return False

    def sub(self, topics: list, qos: int = 1) -> None:
        """
        Subscribe to a broker/topic(s)

        :param topic: `list` of topic(s)
        :param qos: `int` of quality of service (0, 1, 2)

        :returns: `None`
        """

        def on_connect(client, userdata, flags, reason_code, properties):
            LOGGER.debug(f'Connected to broker {self.broker_safe_url}')
            LOGGER.debug(f'Subscribing to topics {topics}')
            for topic in topics:
                client.subscribe(topic, qos=qos)
                LOGGER.debug(f'Subscribed to topic {topic}, qos {qos}')

        def on_disconnect(client, userdata, flags, reason_code, properties):
            msg = f'Disconnected from {self.broker_safe_url}: ({reason_code})'
            LOGGER.debug(msg)

        LOGGER.debug(f'Subscribing to broker {self.broker_safe_url}, topic(s) {topics}')  # noqa
        self.conn.on_connect = on_connect
        self.conn.on_disconnect = on_disconnect
        self.conn.loop_forever()

    def close(self) -> None:
        """
        Close MQTT connection

        :returns: `None`
        """

        self.conn.disconnect()

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
