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

__version__ = '0.3.dev0'

import click

from pywis_pubsub.publish import publish
from pywis_pubsub.schema import schema
from pywis_pubsub.subscribe import subscribe
from pywis_pubsub.validation import message


@click.group()
@click.version_option(version=__version__)
def cli():
    """WIS2 Publish/Subscribe/Download utility"""

    pass


cli.add_command(message)
cli.add_command(publish)
cli.add_command(schema)
cli.add_command(subscribe)
