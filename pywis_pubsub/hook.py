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

from abc import ABC, abstractmethod
import importlib
import logging

from paho.mqtt import client as mqtt_client

LOGGER = logging.getLogger(__name__)


class Hook(ABC):
    # @abstractmethod
    def __init__(self, client: mqtt_client = None):
        """
        Initializer

        :param client: optional `paho.mqtt.client` object.
                       If this parameter is passed, a hook implementation
                       publishing messages can reuse an existing passed client
                       session (i.e. `self.client`).  Passing a client allows
                       a hook implmeentation to better manage conections for
                       advanced applications if required.

        :returns: `pywis_pubsub.hook.Hook`
        """

        self.client = client

    @abstractmethod
    def execute(self, topic: str, msg_dict: dict) -> None:
        """
        Execute a hook

        :param topic: `str` of topic
        :param msg_dict: `dict` of message payload

        :returns: `None`
        """

        raise NotImplementedError()


class TestHook(Hook):
    def execute(self, topic: str, msg_dict: dict) -> None:
        LOGGER.debug(f"Hi from test hook!  Topic: {topic}, Message id: {msg_dict['id']}")  # noqa


def load_hook(factory: str) -> Hook:
    """
    Load hook plugin

    :param factory: `str` of dotted path of Python module/class

    :returns: hook object
    """

    modulename, classname = factory.rsplit('.', 1)

    LOGGER.debug(f'module name: {modulename}')
    LOGGER.debug(f'class name: {classname}')

    module = importlib.import_module(modulename)

    return getattr(module, classname)()
