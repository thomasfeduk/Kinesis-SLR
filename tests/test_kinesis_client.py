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

    @patch('includes.kinesis_client.Client._get_records', create=True)
    @patch('includes.kinesis_client.Client._shard_iterator', create=True)
    @patch('os.path.exists', create=True)
    @patch('includes.kinesis_client.Client._confirm_shards_exist', create=True)
    @patch('includes.kinesis_client.Client._get_shard_ids_of_stream', create=True)
    @patch('includes.kinesis_client.Client._scrape_records_for_shard_handle_poll_delay', create=True)
    @patch('includes.kinesis_client.Client._is_valid', create=True)
    def test_simple_none(self,
                         mocked_is_valid,
                         mocked_poll,
                         mocked_get_shard_ids_of_stream,
                         mocked_confirm_shards_exist,
                         mocked_os_path_exists,
                         mocked_shard_iterator,
                         mocked_get_records,
                         ):

        mocked_get_shard_ids_of_stream.return_value = ['shard_test']
        mocked_os_path_exists.return_value = False
        mocked_shard_iterator.return_value = 'the_iter_id'
        mocked_get_records.return_value = 'the_iter_id'

        config = mock.Mock()
        config.shard_ids = []

        client = kinesis.Client(config)

        client.begin_scraping()
        die('est here 40')

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
