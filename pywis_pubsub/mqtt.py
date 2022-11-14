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

import logging
import random
from typing import Any, Callable

from copy import deepcopy
from urllib.parse import urlparse
from paho.mqtt import client as mqtt_client
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
        LOGGER.debug('Connected to broker')

    def pub(self, topic: str, message: str, qos: int = 1) -> bool:
        """
        Publish a message to a broker/topic

        :param topic: `str` of topic
        :param message: `str` of message

        :returns: `bool` of publish result
        """

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

        def on_connect(client, userdata, flags, rc):
            LOGGER.debug(f'Connected to broker {self.broker_safe_url}')
            LOGGER.debug(f'Subscribing to topics {topics}')
            for topic in topics:
                client.subscribe(topic, qos=qos)
                LOGGER.debug(f'Subscribed to topic {topic}, qos {qos}')

        def on_disconnect(client, userdata, rc):
            LOGGER.debug(f'Disconnected from {self.broker}')

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
