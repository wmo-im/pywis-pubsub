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

import os
import unittest

TESTDATA_DIR = os.path.dirname(os.path.realpath(__file__))


def get_abspath(filepath):
    """helper function to facilitate absolute test file access"""

    return os.path.join(TESTDATA_DIR, filepath)


def msg(test_id, test_description):
    """convenience function to print out test id and desc"""
    return f'{test_id}: {test_description}'


class PyWISPubSubTest(unittest.TestCase):
    """Test suite for package pywis_pubsub"""
    def setUp(self):
        """setup test fixtures, etc."""
        print(msg(self.id(), self.shortDescription()))

    def tearDown(self):
        """return to pristine state"""
        pass

    def test_smoke(self):
        """Simple Smoke Test"""
        # test assertions go here
        self.assertEquals(1, 1, 'Expected equality')


if __name__ == '__main__':
    unittest.main()
