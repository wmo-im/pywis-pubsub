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

import json
import os
import unittest
from unittest.mock import patch

from requests import Session
from pywis_pubsub.ets import WNMTestSuite
from pywis_pubsub.validation import validate_message
from pywis_pubsub.verification import verify_data

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

    def test_validation(self):
        """Test validation"""

        with open(get_abspath('test_valid.json')) as fh:
            data = json.load(fh)
            is_valid, errors = validate_message(data)
            self.assertTrue(is_valid)

        with open(get_abspath('test_invalid.json')) as fh:
            data = json.load(fh)
            is_valid, errors = validate_message(data)
            self.assertFalse(is_valid)

        with open(get_abspath('test_malformed.json')) as fh:
            with self.assertRaises(json.decoder.JSONDecodeError):
                data = json.load(fh)
                is_valid, errors = validate_message(data)

    @patch.object(Session, 'get')
    def test_verification(self, mock_get):
        """Test verification"""

        data_filename = 'A_SZIO01AMMC110648_C_EDZW_20230811064904_50549867'

        with open(get_abspath(data_filename), 'rb') as fh:
            mock_get.return_value.status_code = 200
            mock_get.return_value.content = fh.read()

        with open(get_abspath('test_valid_checksum.json')) as fh:
            data = json.load(fh)
            is_valid = verify_data(data, True)
            self.assertTrue(is_valid)


class WNMETSTest(unittest.TestCase):
    """WNM tests of tests"""

    def setUp(self):
        """setup test fixtures, etc."""
        pass

    def tearDown(self):
        """return to pristine state"""
        pass

    def test_pass(self):
        """Simple tests for a passing record"""
        with open(get_abspath('test_valid.json')) as fh:
            ts = WNMTestSuite(json.load(fh))
            results = ts.run_tests()

            codes = [r['code'] for r in results['ets-report']['tests']]

            self.assertEqual(codes.count('FAILED'), 0)
            self.assertEqual(codes.count('PASSED'), 8)
            self.assertEqual(codes.count('SKIPPED'), 0)

    def test_fail(self):
        """Simple tests for a failing record"""
        with open(get_abspath('test_invalid_datetime.json')) as fh:
            record = json.load(fh)
            ts = WNMTestSuite(record)
            results = ts.run_tests()

            codes = [r['code'] for r in results['ets-report']['tests']]

            self.assertEqual(codes.count('FAILED'), 1)
            self.assertEqual(codes.count('PASSED'), 7)
            self.assertEqual(codes.count('SKIPPED'), 0)

            with self.assertRaises(ValueError):
                ts.run_tests(fail_on_schema_validation=True)


if __name__ == '__main__':
    unittest.main()
