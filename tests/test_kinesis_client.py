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
            kinesis.ClientConfig(config_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: stream_name',
            str(ex.exception)
        )

    def test_ReqConfigs(self):
        # Must match the same list order declared in kinesis.ConfigClient._is_valid
        required_configs = [
            'stream_name',
            'shard_ids',
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
            if i == len(required_configs) - 1:
                return
            config_input[current_conf] = "x"
            with self.assertRaises(ValueError) as ex:
                kinesis.ClientConfig(config_input)
            self.assertEqual(
                f'config-kinesis_scraper.yaml: Missing config parameter: {required_configs[i + 1]}',
                str(ex.exception)
            )
            i += 1

    def test_stream_name(self):
        config_input = {
            "stream_name": 5,
            "shard_ids": 5,
            "starting_position": 5,
            "max_total_records_per_shard": 5,
            "poll_batch_size": 5,
            "poll_delay": 5,
            "max_empty_record_polls": 5,
        }
        # Wrong type
        with self.assertRaises(TypeError) as ex:
            kinesis.ClientConfig(config_input)
        self.assertEqual("stream_name must be a string. Type provided: <class 'int'>",
                         str(ex.exception)
                         )
        # Blank
        config_input["stream_name"] = ""
        with self.assertRaises(ValueError) as ex:
            kinesis.ClientConfig(config_input)
        self.assertEqual("config-kinesis_scraper.yaml: A stream name must be set.",
                         str(ex.exception)
                         )
        # Default name
        config_input["stream_name"] = "stream_name_here"
        with self.assertRaises(ValueError) as ex:
            kinesis.ClientConfig(config_input)
        self.assertEqual("config-kinesis_scraper.yaml: A stream name must be set.",
                         str(ex.exception)
                         )

    def test_shard_ids(self):
        config_input = {
            "stream_name": "kinesis_slr",
            "shard_ids": 5,
            "starting_position": 5,
            "max_total_records_per_shard": 5,
            "poll_batch_size": 5,
            "poll_delay": 5,
            "max_empty_record_polls": 5,
        }
        # Wrong type on dict value
        with self.assertRaises(TypeError) as ex:
            kinesis.ClientConfig(config_input)
        self.assertEqual("shard_ids must be of type list if specified. Type provided: <class 'int'>",
                         str(ex.exception)
                         )

        # Correct dict value (list), but wrong list values
        config_input = {
            "stream_name": "kinesis_slr",
            "shard_ids": [7],
            "starting_position": 5,
            "max_total_records_per_shard": 5,
            "poll_batch_size": 5,
            "poll_delay": 5,
            "max_empty_record_polls": 5,
        }
        with self.assertRaises(TypeError) as ex:
            kinesis.ClientConfig(config_input)
        self.assertEqual("Each shard_id must be a string. Value provided: <class 'int'> 7",
                         str(ex.exception)
                         )

    def test_shard_ids_validate_shard_ids(self):
        self.assertEqual([], kinesis.ClientConfig.validate_shard_ids())

    def test_validate_iterator_types(self):
        self.assertEqual([], kinesis.ClientConfig.validate_shard_ids())


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
