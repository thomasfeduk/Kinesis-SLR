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

    def test_simple_none(self):
        with patch('includes.kinesis_client._scrape_records_for_shard_handle_poll_delay', create=True) as mocked_poll:
            with patch.object(kinesis.Client, '_get_records',return_value="fake_temp") as mocked_get_records:
                obj = mock.Mock()
                iter_obj = mock.Mock()
                iter_obj.loop_count = 0
                # iter_obj.shard_id = 'abc'
                # iter_obj.shard_iterator = 'zzzz'
                # mocked_get_records.return_value = 'list of recordds'
                kinesis.Client._scrape_records_for_shard_iterator(obj, iter_obj)
                # obj.scrape(obj, iter_obj)
                # Mock out the entire init so we can just test a specific method and manually set its needed attribs
                # with patch.object(kinesis.Client, "__init__", lambda x, y, z: None):
                #     c = kinesis.Client('a','b')
                #     pvdd(c._scrape_records_for_shard_iterator('a'))

    # def test_ReqConfigs_empty(self):
    #     with patch('includes.kinesis_client.ShardIteratorConfig.is_valid', create=True) as mocked_kinesis_client:
    #         mocked_kinesis_client.return_value = 'boo'
    #
    #     _scrape_records_for_shard_iterator


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
