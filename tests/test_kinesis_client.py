import json
import importlib
import unittest
import unittest.mock as mock
from unittest.mock import patch
import includes.kinesis_client as kinesis
import sys
from includes.debug import *
import includes.common as common


class ClientConfig(unittest.TestCase):
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
        # Run the test once with an empty input config list to simulate no configs set
        config_input = {}
        with self.assertRaises(ValueError) as ex:
            kinesis.ConfigClient(config_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: stream_name',
            str(ex.exception)
        )

    def test_ReqConfigs(self):
        # Must match the same list order declared in kinesis.ConfigClient._is_valid
        required_configs = [
            'stream_name',
            'shardIds',
            'starting_position',
            'max_total_records_per_shard',
            'poll_batch_size',
            'poll_delay',
            'max_empty_record_polls',
        ]
        config_input = {}
        i = 0
        for current_conf in required_configs:
            # End the test if we already ran through all but the last item,
            # since the last item is checked in the second to last call (index+1)
            if i == len(required_configs)-1:
                return
            config_input[current_conf] = "x"
            with self.assertRaises(ValueError) as ex:
                kinesis.ConfigClient(config_input)
            self.assertEqual(
                f'config-kinesis_scraper.yaml: Missing config parameter: {required_configs[i+1]}',
                str(ex.exception)
            )
            i += 1

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
