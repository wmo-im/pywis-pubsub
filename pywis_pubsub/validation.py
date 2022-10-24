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
from typing import Tuple

from jsonschema import RefResolver, validate

from pywis_pubsub.util import MESSAGE_SCHEMA

LOGGER = logging.getLogger(__name__)


def validate_message(instance: dict) -> Tuple[bool, str]:
    """
    Validate a JSON instance document against an JSON schema

    :param instance: `dict` of JSON

    :return: `tuple` of `bool` of validation result
             and `str` of error message(s)
    """

    success = False
    error_message = None

    schema = str(MESSAGE_SCHEMA)
    schema_dir = MESSAGE_SCHEMA.parent
    resolver = RefResolver(base_uri=f'file://{schema_dir}/',
                           referrer=schema)

    try:
        validate(instance, schema, resolver=resolver)
        success = True
    except Exception as err:
        import traceback
        print(traceback.format_exc())
        error_message = err

    return (success, error_message)
