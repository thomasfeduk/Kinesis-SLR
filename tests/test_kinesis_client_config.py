import json
import includes.exceptions as exceptions
import botocore
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
        self.boto_client = mock.MagicMock(spec=botocore.client.BaseClient)
        self.config_input = {
            'debug_level': "INFO",
            'stream_name': "user_activities",
            'shard_ids': "shard-001",
            'starting_position': "TRIM_HORIZON",
            'starting_timestamp': "2022-12-01 00:00:00",
            'starting_sequence_number': "abc",
            'ending_position': "LATEST",
            'ending_timestamp': "2022-12-01 00:00:00",
            'ending_sequence_number': "xyz",
            'total_records_per_shard': 500,
            'poll_batch_size': 100,
            'poll_delay': 0,
            'max_empty_polls': 5,
        }

    def tearDown(self):
        pass

    def test_ReqConfigs_empty(self):
        # Run the test once with an empty input config list to simulate no configs set
        config_input = {}
        with self.assertRaises(expected_exception=exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(config_input, self.boto_client)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: debug_level',
            str(ex.exception)
        )

    def test_ReqConfigs(self):
        # Must match the same list order declared in kinesis.ConfigClient._is_valid
        # Delete optional entries so we can check the proper exceptions are thrown in order for parameter names
        del self.config_input['starting_timestamp']
        del self.config_input['starting_sequence_number']
        del self.config_input['ending_timestamp']
        del self.config_input['ending_sequence_number']
        del self.config_input['total_records_per_shard']
        required_configs = list(self.config_input.keys())

        config_input = {}
        i = 0
        for current_conf in required_configs:
            # End the test if we already ran through all but the last item,
            # since the last item is checked in the second to last call (index+1)
            if i == len(required_configs) - 1:
                return
            config_input[current_conf] = "x"
            with self.assertRaises(exceptions.ConfigValidationError) as ex:
                kinesis.ClientConfig(config_input, self.boto_client)
            self.assertEqual(
                f'config-kinesis_scraper.yaml: Missing config parameter: {required_configs[i + 1]}',
                str(ex.exception)
            )
            i += 1

    def test_stream_name_invalid_int(self):
        self.config_input["stream_name"] = 5
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("stream_name must be a string. Type provided: <class 'int'>",
                         str(ex.exception)
                         )

    def test_stream_name_invalid_blank(self):
        self.config_input["stream_name"] = ""
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("config-kinesis_scraper.yaml: A stream name must be set.",
                         str(ex.exception)
                         )

    def test_stream_name_invalid_default_name(self):
        self.config_input["stream_name"] = "stream_name_here"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("config-kinesis_scraper.yaml: A stream name must be set.",
                         str(ex.exception)
                         )

    def test_shard_ids_invlaid_int(self):
        self.config_input["shard_ids"] = 5
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("shard_ids must be of type list if specified. Type provided: <class 'int'>",
                         str(ex.exception)
                         )

    def test_shard_ids_invlaid_list_int(self):
        self.config_input["shard_ids"] = [7]
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("Each shard_id must be a string. Value provided: <class 'int'> 7",
                         str(ex.exception)
                         )

    def test_shard_ids_validate_shard_ids(self):
        self.assertEqual([], kinesis.ClientConfig.validate_shard_ids())

