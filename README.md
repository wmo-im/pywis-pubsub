[![flake8](https://github.com/wmo-im/pywis-pubsub/workflows/flake8/badge.svg)](https://github.com/wmo-im/pywis-pubsub/actions)

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
git clone https://github.com/geopython/pywis-pubsub.git
cd pywis-pubsub
python3 setup.py build
python3 setup.py install
```

## Running

```bash
cp pywis-pubsub-config-example.yml local.yml
vi local.yml # update accordingly

pywis-pubsub --version

# sync WIS2 notification schema
pywis-pubsub schema sync

# connect, and simply echo messages
pywis-pubsub subscribe --config local.yml

# connect, and download messages
pywis-pubsub subscribe --config local.yml --download

# connect, and filter messages by geometry
pywis-pubsub subscribe --config local.yml --bbox=-142,42,-52,84

# connect, and filter messages by geometry, increase debugging verbosity
pywis-pubsub subscribe --config local.yml --bbox=-142,42,-52,84 --verbosity=DEBUG
```

### Using the API

```python
# Python API examples go here

from pywis_pubsub.subscribe import MQTTPubSubClient

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
