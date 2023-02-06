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

LOGGER = logging.getLogger(__name__)


class Hook(ABC):
    # @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def execute(self, msg_dict: dict) -> None:
        """
        Execute a hook

        :param payload: `dict` of message payload

        :returns: `None`
        """

        raise NotImplementedError()


class TestHook(Hook):
    def execute(self, msg_dict: dict) -> None:
        LOGGER.debug(f"Hi from test hook!  Message id: {msg_dict['id']}")


def load_hook(factory) -> Hook:
    """
    Load hook plugin

    :param factory: dotted path of Python module/class

    :returns: hook object
    """

    modulename, classname = factory.rsplit('.', 1)

    LOGGER.debug(f'module name: {modulename}')
    LOGGER.debug(f'class name: {classname}')

    module = importlib.import_module(modulename)

    return getattr(module, classname)()
