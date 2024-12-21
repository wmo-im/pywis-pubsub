[![flake8](https://github.com/wmo-im/pywis-pubsub/workflows/flake8/badge.svg)](https://github.com/wmo-im/pywis-pubsub/actions)
[![test-publish-subscribe-download](https://github.com/wmo-im/pywis-pubsub/workflows/test-publish-subscribe-download/badge.svg)](https://github.com/wmo-im/pywis-pubsub/actions)

# pywis-pubsub

## Overview

pywis-pubsub provides subscription and download capability of data from WIS2.

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

Create configuration

```bash
cp pywis-pubsub-config.yml local.yml
vim local.yml # update accordingly to configure subscribe options
```

### Subscribing

```bash
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

### Validating a message and verifying data

```bash
# validate a message
pywis-pubsub message validate /path/to/message1.json

# verify data from a message
pywis-pubsub message verify /path/to/message1.json

# validate WNM against abstract test suite (file on disk)
pywis-pubsub ets validate /path/to/file.json

# validate WNM against abstract test suite (URL)
pywis-pubsub ets validate https://example.org/path/to/file.json

# validate WNM against abstract test suite (URL), but turn JSON Schema validation off
pywis-pubsub ets validate https://example.org/path/to/file.json --no-fail-on-schema-validation

# key performance indicators

# set environment variable for GDC URL
export PYWIS_PUBSUB_GDC_URL=https://api.weather.gc.ca/collections/wis2-discovery-metadata

# all key performance indicators at once
pywis-pubsub kpi validate https://example.org/path/to/file.json --verbosity DEBUG

# all key performance indicators at once, but turn ETS validation off
pywis-pubsub kpi validate https://example.org/path/to/file.json --no-fail-on-ets --verbosity DEBUG

# all key performance indicators at once, in summary
pywis-pubsub kpi validate https://example.org/path/to/file.json --verbosity DEBUG --summary

# selected key performance indicator
pywis-pubsub kpi validate --kpi metadata_id /path/to/file.json -v INFO
```

### Publishing

```bash
cp pub-config-example.yml pub-local.yml
vim pub-local.yml # update accordingly to configure publishing options

# example publishing a WIS2 notification message with attributes: 
# unique-id=stationXYZ-20221111085500 
# data-url=http://www.meteo.xx/stationXYZ-20221111085500.bufr4 
# lon,lat,elevation=33.8,11.8,112
# wigos_station_identifier=0-20000-12345
pywis-pubsub publish --topic origin/a/wis2/centre-id/data/core/weather --config pub-local.yml -i stationXYZ-20221111085500 -u https://example.org/stationXYZ-20221111085500.bufr4 -g 33.8,-11.8,8.112 -w 0-20000-12345

# publish a message with a WCMP2 metadata id
pywis-pubsub publish --topic origin/a/wis2/centre-id/data/core/weather --config pub-local.yml -i stationXYZ-20221111085500 -u https://example.org/stationXYZ-20221111085500.bufr4 -g 33.8,-11.8,8.112 -w 0-20000-12345 --metadata-id "x-urn:wmo:md:test-foo:htebmal2001"

# publish a message with a datetime (instant)
pywis-pubsub publish --topic origin/a/wis2/centre-id/data/core/weather --config pub-local.yml -i stationXYZ-20221111085500 -u https://example.org/stationXYZ-20221111085500.bufr4 -g 33.8,-11.8,8.112 -w 0-20000-12345 --metadata-id "x-urn:wmo:md:test-foo:htebmal2001" --datetime 2024-01-08T22:56:23Z

# publish a message with a start and end datetime (extent)
pywis-pubsub publish --topic origin/a/wis2/centre-id/data/core/weather --config pub-local.yml -i stationXYZ-20221111085500 -u https://example.org/stationXYZ-20221111085500.bufr4 -g 33.8,-11.8,8.112 -w 0-20000-12345 --metadata-id "x-urn:wmo:md:test-foo:htebmal2001" --datetime 2024-01-08T20:56:23Z/2024-01-08T22:56:43Z

# publish a message as a data update
pywis-pubsub publish --topic origin/a/wis2/centre-id/data/core/weather --config pub-local.yml -i stationXYZ-20221111085500 -u https://example.org/stationXYZ-20221111085500.bufr4 -g 33.8,-11.8,8.112 -w 0-20000-12345 --metadata-id "x-urn:wmo:md:test-foo:htebmal2001" --operation update

# publish a message as a data deletion
pywis-pubsub publish --topic origin/a/wis2/centre-id/data/core/weather --config pub-local.yml -i stationXYZ-20221111085500 -u https://example.org/stationXYZ-20221111085500.bufr4 -g 33.8,-11.8,8.112 -w 0-20000-12345 --metadata-id "x-urn:wmo:md:test-foo:htebmal2001" --operation delete

# publish a message from file on disk
pywis-pubsub publish --topic origin/a/wis2/centre-id/data/core/weather --config pub-local.yml --file my_message.json
```

### Using the API

Python examples:

Subscribing to a WIS2 Global Broker
```python
from pywis_pubsub.mqtt import MQTTPubSubClient

options = {
    'storage': {
        'type': 'fs',
        'basedir': '/tmp'
    },
    'bbox': [-90, -180, 90, 180]
}
topics = [
    'topic1',
    'topic2'
]

m = MQTTPubSubClient('mqtt://localhost:1883', options)
# example with credentials
# m = MQTTPubSubClient('mqtt://username:password@localhost:1883', options)

m.sub(topics)
```

Publishing a WIS2 Notification Message
```python
from datetime import datetime, timezone

from pywis_pubsub.mqtt import MQTTPubSubClient
from pywis_pubsub.publish import create_message

message = create_message(
        topic='foo/bar',
        content_type='application/bufr',
        url='http://www.meteo.xx/stationXYZ-20221111085500.bufr4', 
        identifier='stationXYZ-20221111085500', 
        datetime_=datetime.now(timezone.utc),
        geometry=[33.8, -11.8, 123],
        metadata_id='x-urn:wmo:md:test-foo:htebmal2001',
        wigos_station_identifier='0-20000-12345',
        operation='update'
)

m = MQTTPubSubClient('mqtt://localhost:1883')
m.pub(topic, json.dumps(message))
```

Running KPIs
```pycon
>>> # test KPI
>>> import json
>>> from pywis_pubsub.kpi import WNMKeyPerformanceIndicators
>>> with open('/path/to/file.json') as fh:
...     data = json.load(fh)
>>> kpis = WNMKeyPerformanceIndicators(data)
>>> results = kpis.evaluate()
>>> results['summary']
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
# create release (x.y.z is the release version)
vi pywis_pubsub/__init__.py  # update __version__
vi debian/changelog  # add changelog entry
git commit -am 'update release version x.y.z'
git push origin main
git tag -a x.y.z -m 'tagging release version x.y.z'
git push --tags

# upload to PyPI
rm -fr build dist *.egg-info
python3 setup.py sdist bdist_wheel --universal
twine upload dist/*

# publish release on GitHub (https://github.com/wmo-im/pywis-pubsub/releases/new)

# bump version back to dev
vi pywis_pubsub/__init__.py  # update __version__
git commit -am 'back to dev'
git push origin main
```

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/wmo-im/pywis-pubsub/issues).

## Contact

* [Antje Schremmer](https://github.com/antje-s)
* [Tom Kralidis](https://github.com/tomkralidis)
* [Maaike Limper](https://github.com/maaikelimper)
