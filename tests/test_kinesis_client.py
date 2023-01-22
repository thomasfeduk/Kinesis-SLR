import json
import importlib
import unittest
import unittest.mock as mock
from unittest.mock import patch
import includes.kinesis_client as kinesis
import sys
from includes.debug import *
import includes.common as common


class ClientGetRecords(unittest.TestCase):
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

    def test_ReqConfigs_empty(self):
        with patch('includes.kinesis_client.ShardIteratorConfig.is_valid', create=True) as mocked_kinesis_client:
            mocked_kinesis_client.return_value = 'boo'



        # Run the test once with an empty input config list to simulate no configs set
        config_input = {}
        # with self.assertRaises(ValueError) as ex:
        #     kinesis.ClientConfig(config_input)
        # self.assertEqual(
        #     'config-kinesis_scraper.yaml: Missing config parameter: stream_name',
        #     str(ex.exception)
        # )

    # def test_ClientConfig(self):
    #     with patch('includes.kinesis_client.ShardIteratorConfig.is_valid', create=True) as mocked_kinesis_client:
    #         mocked_kinesis_client.return_value = 'boo'
    #
    #         yaml_input = {
    #
    #         }
    #         kinesis_obj = kinesis.ConfigClient(yaml_input)

    #     pvdd(kinesis_obj)
    #
    # self.assertEqual(1, 2)
