import json
import importlib
import unittest
import unittest.mock as mock
from unittest.mock import patch
import includes.kinesis_client as kinesis_client
import sys
from includes.debug import *


class Main(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_ClientConfig(self):
        with patch('includes.kinesis_client.ShardIteratorConfig.is_valid', create=True) as mocked_kinesis_client:
            mocked_kinesis_client.return_value = 'boo'
            var = kinesis_client.ShardIteratorConfig.is_valid()
            pvdd(var)

        self.assertEqual(1, 2)

