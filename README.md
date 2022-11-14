[![flake8](https://github.com/wmo-im/pywis-pubsub/workflows/flake8/badge.svg)](https://github.com/wmo-im/pywis-pubsub/actions)
[![test-publish-subscribe-download](https://github.com/wmo-im/pywis-pubsub/workflows/test-publish-subscribe-download/badge.svg)](https://github.com/wmo-im/pywis-pubsub/actions)

# pywis-pubsub

## Overview

pywis-pubsub provides subscription and download capability of WMO data from WIS 2.0
infrastructure services.

## Installation

The easiest way to install pywis-pubsub is via the Python [pip](https://pip.pypa.io)
utility:

```bash
pip3 install pywis-pubsub
```

### Requirements
- Python 3
- [virtualenv](https://virtualenv.pypa.io)

### Dependencies
Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during pywis-pubsub installation.

#### Windows installations
Note that you will need Cython and [Shapely Windows wheels](https://pypi.org/project/shapely/#files) for windows for your architecture
prior to installing pywis-pubsub.


### Installing pywis-pubsub

```bash
# setup virtualenv
python3 -m venv --system-site-packages pywis-pubsub
cd pywis-pubsub
source bin/activate

# clone codebase and install
git clone https://github.com/wmo-im/pywis-pubsub.git
cd pywis-pubsub
python3 setup.py install
```

## Running

First check pywis-pubsub was correctly installed

```bash
pywis-pubsub --version
```

### Subscribing

```bash
cp pywis-pubsub-config-example.yml local.yml
vim local.yml # update accordingly to configure subscribe-options

pywis-pubsub --version

# sync WIS2 notification schema
pywis-pubsub schema sync

# connect, and simply echo messages
pywis-pubsub subscribe --config local.yml

# subscribe, and download data from message
pywis-pubsub subscribe --config local.yml --download

# subscribe, and filter messages by geometry
pywis-pubsub subscribe --config local.yml --bbox=-142,42,-52,84

# subscribe, and filter messages by geometry, increase debugging verbosity
pywis-pubsub subscribe --config local.yml --bbox=-142,42,-52,84 --verbosity=DEBUG
```

### Publishing

```bash
cp pub-config-example.yml pub-local.yml
vim pub-local.yml # update accordingly to configure publish-options

# example publishing a WIS2-message with attributes: 
# unique-id=stationXYZ-20221111085500 
# data-url=http://www.meteo.xx/stationXYZ-20221111085500.bufr4 
# lon,lat,elevation=33.8,11.8,112
# wigos_station_identifier=0-20000-12345
pywis-pubsub publish --config pub-local.yml -i stationXYZ-20221111085500 -u http://www.meteo.xx/stationXYZ-20221111085500.bufr4 -g 33.8,-11.8,8.112 -w 0-20000-12345
```

### Using the API

Python examples:

```python
# subscriber example
from pywis_pubsub.mqtt import MQTTPubSubClient

options = {
    'storage': {
        'type': 'fs',
        'path': '/tmp'
    },
    'bbox': [-90, -180, 90, 180]
}
topics = [
    'topic1',
    'topic2'
]

m = MQTTPubSubClient('mqtt://localhost:1883', options)
m.sub(topics)
```

```python
# publish example
from pywis_pubsub.mqtt import MQTTPubSubClient
from pywis_pubsub.publish import prepare_message

message = prepare_message(
        topic='foo/bar',
        content_type='application/x-bufr',
        url='http://www.meteo.xx/stationXYZ-20221111085500.bufr4', 
        identifier='stationXYZ-20221111085500', 
        geometry=[33.8, -11.8, 123],
        wigos_station_identifier='0-20000-12345'
)

m = MQTTPubSubClient('mqtt://localhost:1883')
client.pub(topic, json.dumps(message))
```

## Development

### Running Tests

```bash
# install dev requirements
pip3 install -r requirements-dev.txt

# run tests like this:
python3 tests/run_tests.py

# or this:
python3 setup.py test
```

## Releasing

```bash
rm -fr build dist *.egg-info
python3 setup.py sdist bdist_wheel --universal
twine upload dist/*
```

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/wmo-im/pywis-pubsub/issues).

## Contact

* [Antje Schremmer](https://github.com/antje-s)
* [Tom Kralidis](https://github.com/tomkralidis)
* [Maaike Limper](https://github.com/maaikelimper)
