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

    if userdata.get('download') is not None:
        basepath = userdata['download_dir'] / msg_dict['properties']['hierarchy']  # noqa
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
        options['download_dir'] = config.get('download_dir')

    client = MQTTPubSubClient(broker, options)
    click.echo(f'Connected to broker {client.broker_safe_url}')

    click.echo(f'Subscribing to topics {topics}')
    client.sub(topics, qos)
