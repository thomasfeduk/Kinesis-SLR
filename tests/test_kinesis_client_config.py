import includes.exceptions as exceptions
import botocore
import unittest
import unittest.mock as mock
from unittest.mock import patch
import includes.kinesis_client as kinesis


class ClientConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.boto_client = mock.Mock(spec=botocore.client.BaseClient)
        self.config_input = {
            'debug_level': "INFO",
            'stream_name': "user_activities",
            'shard_ids': ["shardId-000000001"],
            'starting_position': "TRIM_HORIZON",
            'starting_timestamp': "2022-12-01 00:00:00",
            'starting_sequence_number': "111111",
            'ending_position': "LATEST",
            'ending_timestamp': "2022-12-01 00:00:00",
            'ending_sequence_number': "22222",
            'total_records_per_shard': 500,
            'poll_batch_size': 100,
            'poll_delay': 0,
            'max_empty_polls': 5,
        }

    def tearDown(self):
        pass

    def test_valid_default(self):
        self.assertEqual(self.config_input["debug_level"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).debug_level)
        self.assertEqual(self.config_input["stream_name"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).stream_name)
        self.assertEqual(self.config_input["shard_ids"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).shard_ids)
        self.assertEqual(self.config_input["starting_position"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).starting_position)
        # None because starting position != timestamp
        self.assertEqual(None,
                         kinesis.ClientConfig(self.config_input, self.boto_client).starting_timestamp)
        # None because starting position != sequence_number
        self.assertEqual(None,
                         kinesis.ClientConfig(self.config_input, self.boto_client).starting_sequence_number)
        self.assertEqual(self.config_input["ending_position"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_position)
        # None because ending position != timestamp
        self.assertEqual(None,
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_timestamp)
        # None because ending position != sequence_number
        self.assertEqual(None,
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_sequence_number)
        self.assertEqual(self.config_input["total_records_per_shard"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).total_records_per_shard)
        self.assertEqual(self.config_input["poll_batch_size"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).poll_batch_size)
        self.assertEqual(self.config_input["poll_delay"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).poll_delay)
        self.assertEqual(self.config_input["max_empty_polls"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).max_empty_polls)

    def test_valid_starting_timestamp(self):
        self.config_input["starting_position"] = "AT_TIMESTAMP"
        self.assertEqual(self.config_input["starting_position"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).starting_position)
        self.assertEqual(self.config_input["starting_timestamp"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).starting_timestamp)

    def test_valid_starting_sequence_number(self):
        self.config_input["starting_position"] = "AT_SEQUENCE_NUMBER"
        self.assertEqual(self.config_input["starting_position"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).starting_position)
        self.assertEqual(self.config_input["starting_sequence_number"],
                         kinesis.ClientConfig(self.config_input, self.boto_client)._starting_sequence_number)

    def test_valid_starting_position_latest(self):
        self.config_input["starting_position"] = "LATEST"
        self.assertEqual(self.config_input["starting_position"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).starting_position)

    def test_valid_ending_position_latest(self):
        self.config_input["ending_position"] = "LATEST"
        self.assertEqual(self.config_input["ending_position"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_position)

    def test_valid_ending_at_timestamp(self):
        self.config_input["ending_position"] = "AT_TIMESTAMP"
        self.assertEqual(self.config_input["ending_timestamp"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_timestamp)

    def test_valid_ending_before_timestamp(self):
        self.config_input["ending_position"] = "BEFORE_TIMESTAMP"
        self.assertEqual(self.config_input["ending_timestamp"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_timestamp)

    def test_valid_ending_after_timestamp(self):
        self.config_input["ending_position"] = "AFTER_TIMESTAMP"
        self.assertEqual(self.config_input["ending_timestamp"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_timestamp)

    def test_valid_ending_at_sequence_number(self):
        self.config_input["ending_position"] = "AT_SEQUENCE_NUMBER"
        self.assertEqual(self.config_input["ending_sequence_number"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_sequence_number)

    def test_valid_ending_before_sequence_number(self):
        self.config_input["ending_position"] = "BEFORE_SEQUENCE_NUMBER"
        self.assertEqual(self.config_input["ending_sequence_number"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_sequence_number)

    def test_valid_ending_after_sequence_number(self):
        self.config_input["ending_position"] = "AFTER_SEQUENCE_NUMBER"
        self.assertEqual(self.config_input["ending_sequence_number"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).ending_sequence_number)

    def test_invalid_starting_at_timestamp_type(self):
        self.config_input["starting_position"] = "AT_TIMESTAMP"
        self.config_input["starting_timestamp"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: Invalid format for config parameter \"starting_timestamp\".\n"
                      "Format should be YYYY-MM-DD HH:MM:SS. Value provided: <class 'str'> 'abc'", str(ex.exception))
        self.config_input["starting_timestamp"] = 5.5
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: Invalid format for config parameter \"starting_timestamp\".\n"
                      "Format should be YYYY-MM-DD HH:MM:SS. Value provided: <class 'float'> 5.5", str(ex.exception))

    def test_invalid_starting_at_sequence_number_type(self):
        self.config_input["starting_position"] = "AT_SEQUENCE_NUMBER"
        self.config_input["starting_sequence_number"] = {}
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"starting_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'dict'> {}",
                      str(ex.exception))
        self.config_input["starting_position"] = "AT_SEQUENCE_NUMBER"
        self.config_input["starting_sequence_number"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"starting_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'str'> 'abc'",
                      str(ex.exception))

    def test_invalid_starting_before_sequence_number_type(self):
        self.config_input["starting_position"] = "AFTER_SEQUENCE_NUMBER"
        self.config_input["starting_sequence_number"] = {}
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"starting_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'dict'> {}",
                      str(ex.exception))

    def test_invalid_ending_at_timestamp_type(self):
        self.config_input["ending_position"] = "AT_TIMESTAMP"
        self.config_input["ending_timestamp"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: Invalid format for config parameter \"ending_timestamp\".\n"
                      "Format should be YYYY-MM-DD HH:MM:SS. Value provided: <class 'str'> 'abc'", str(ex.exception))

    def test_invalid_ending_before_timestamp_type(self):
        self.config_input["ending_position"] = "BEFORE_TIMESTAMP"
        self.config_input["ending_timestamp"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: Invalid format for config parameter \"ending_timestamp\".\n"
                      "Format should be YYYY-MM-DD HH:MM:SS. Value provided: <class 'str'> 'abc'", str(ex.exception))

    def test_invalid_ending_after_timestamp_type(self):
        self.config_input["ending_position"] = "AFTER_TIMESTAMP"
        self.config_input["ending_timestamp"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: Invalid format for config parameter \"ending_timestamp\".\n"
                      "Format should be YYYY-MM-DD HH:MM:SS. Value provided: <class 'str'> 'abc'", str(ex.exception))
        self.config_input["ending_position"] = "AFTER_TIMESTAMP"
        self.config_input["ending_timestamp"] = []
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: Invalid format for config parameter \"ending_timestamp\".\n"
                      "Format should be YYYY-MM-DD HH:MM:SS. Value provided: <class 'list'> []", str(ex.exception))

    def test_invalid_ending_at_sequence_number_type(self):
        self.config_input["ending_position"] = "AT_SEQUENCE_NUMBER"
        self.config_input["ending_sequence_number"] = {}
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"ending_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'dict'> {}",
                      str(ex.exception))
        self.config_input["ending_position"] = "AT_SEQUENCE_NUMBER"
        self.config_input["ending_sequence_number"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"ending_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'str'> 'abc'",
                      str(ex.exception))

    def test_invalid_ending_before_sequence_number_type(self):
        self.config_input["ending_position"] = "BEFORE_SEQUENCE_NUMBER"
        self.config_input["ending_sequence_number"] = {}
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"ending_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'dict'> {}",
                      str(ex.exception))
        self.config_input["ending_position"] = "BEFORE_SEQUENCE_NUMBER"
        self.config_input["ending_sequence_number"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"ending_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'str'> 'abc'",
                      str(ex.exception))

    def test_invalid_ending_after_sequence_number_type(self):
        self.config_input["ending_position"] = "AFTER_SEQUENCE_NUMBER"
        self.config_input["ending_sequence_number"] = {}
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"ending_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'dict'> {}",
                      str(ex.exception))
        self.config_input["ending_position"] = "AFTER_SEQUENCE_NUMBER"
        self.config_input["ending_sequence_number"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("config-kinesis_scraper.yaml: If \"ending_position\" is *_SEQUENCE_NUMBER, the value must be "
                      "a positive numeric string, float or an integer.\nValue provided: <class 'str'> 'abc'",
                      str(ex.exception))

    def test_valid_validate_shard_ids_none_void(self):
        self.assertEqual([], kinesis.ClientConfig.validate_shard_ids())
        self.assertEqual([], kinesis.ClientConfig.validate_shard_ids(None))

    def test_invalid_shard_ids_empty_element(self):
        self.config_input["shard_ids"] = [""]
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn(
            "Invalid shard_id format. Expected pattern: <class 'str'> 'shardId-XXXXXXX' Received: <class 'str'> ''",
            str(ex.exception))

    def test_invalid_sequence_number_no_shard_ids(self):
        positions_complete = {
            "starting": [
                "AT_SEQUENCE_NUMBER",
                "AFTER_SEQUENCE_NUMBER",
            ],
            "ending": [
                'AT_SEQUENCE_NUMBER',
                'AFTER_SEQUENCE_NUMBER',
                'BEFORE_SEQUENCE_NUMBER',
            ]
        }

        positions = list(positions_complete.keys())

        self.config_input["shard_ids"] = []
        for position in positions:
            for sequence_index in positions_complete[position]:
                self.config_input[f"{position}_position"] = sequence_index
                if position == "starting":
                    self.config_input["ending_position"] = "LATEST"
                else:
                    self.config_input["starting_position"] = "LATEST"

                with self.assertRaises(exceptions.ConfigValidationError) as ex:
                    kinesis.ClientConfig(self.config_input, self.boto_client)
                self.assertIn(
                    f"config-kinesis_scraper.yaml: If \"{position}_position\" is *_SEQUENCE_NUMBER, "
                    "exactly 1 shard_id must be specified as the sequence numbers are unique per shard."
                    "\nValue provided: <class 'list'> []",
                    str(ex.exception))

    def test_invalid_sequence_number_multiple_shard_ids(self):
        positions_complete = {
            "starting": [
                "AT_SEQUENCE_NUMBER",
                "AFTER_SEQUENCE_NUMBER",
            ],
            "ending": [
                'AT_SEQUENCE_NUMBER',
                'AFTER_SEQUENCE_NUMBER',
                'BEFORE_SEQUENCE_NUMBER',
            ]
        }

        positions = list(positions_complete.keys())

        self.config_input["shard_ids"] = ["shardId-1", "shardId-2"]
        for position in positions:
            for sequence_index in positions_complete[position]:
                self.config_input[f"{position}_position"] = sequence_index
                if position == "starting":
                    self.config_input["ending_position"] = "LATEST"
                else:
                    self.config_input["starting_position"] = "LATEST"

                with self.assertRaises(exceptions.ConfigValidationError) as ex:
                    kinesis.ClientConfig(self.config_input, self.boto_client)
                self.assertIn(
                    f"config-kinesis_scraper.yaml: If \"{position}_position\" is *_SEQUENCE_NUMBER, "
                    "exactly 1 shard_id must be specified as the sequence numbers are unique per shard."
                    "\nValue provided: <class 'list'> ['shardId-1', 'shardId-2']",
                    str(ex.exception))

    def test_invalid_sequence_number_positions(self):
        positions = ["starting", "ending"]
        for position in positions:
            self.config_input["starting_position"] = "LATEST"
            self.config_input["ending_position"] = "LATEST"
            self.config_input[f"{position}_position"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn(f"config-kinesis_scraper.yaml: {position}_position must be one of: ['TOTAL_RECORDS_PER_SHARD', "
                      "'AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER', 'BEFORE_SEQUENCE_NUMBER', 'AT_TIMESTAMP', "
                      "'BEFORE_TIMESTAMP', 'AFTER_TIMESTAMP', 'LATEST']\nValue provided: <class 'str'> 'abc'",
                      str(ex.exception))

    def test_boto3_invalid_object(self):
        # Run the test once with an empty input config list to simulate no configs set
        config_input = {}
        with self.assertRaises(expected_exception=exceptions.InvalidArgumentException) as ex:
            kinesis.ClientConfig(config_input, object)
        self.assertEqual(
            "A boto3 Kinesis client object is required. Example: \"boto3.client('kinesis')\". "
            "Value provided: <class 'type'> <class 'object'>",
            str(ex.exception)
        )

    def test_ReqConfigs_empty(self):
        # Run the test once with an empty input config list to simulate no configs set
        config_input = {}
        with self.assertRaises(expected_exception=exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(config_input, self.boto_client)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: debug_level',
            str(ex.exception)
        )

    def test_ReqConfigs_min_specified(self):
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

    def test_debug_level_invalid_wrong_value(self):
        self.config_input["debug_level"] = "amazing"
        with self.assertRaises(expected_exception=exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual(f"config-kinesis_scraper.yaml: debug_level must be one of: "
                         f"['DEBUG', 'INFO', 'WARNING', 'ERROR']\nValue provided: <class 'str'> 'amazing'",
                         str(ex.exception))

    def test_stream_name_invalid_int(self):
        self.config_input["stream_name"] = 5
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("stream_name must be a string. Type provided: <class 'int'>", str(ex.exception))

    def test_stream_name_invalid_blank(self):
        self.config_input["stream_name"] = ""
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("config-kinesis_scraper.yaml: A stream name must be set.", str(ex.exception))

    def test_stream_name_invalid_default_name(self):
        self.config_input["stream_name"] = "stream_name_here"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("config-kinesis_scraper.yaml: A stream name must be set.", str(ex.exception))

    def test_shard_ids_invalid_int(self):
        self.config_input["shard_ids"] = 5
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("shard_ids must be of type list if specified. Type provided: <class 'int'> 5",
                         str(ex.exception))

    def test_shard_ids_invalid_list_int(self):
        self.config_input["shard_ids"] = [7]
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("Each shard_id must be a string. Type <class 'str'> expected. Received: <class 'int'> 7",
                         str(ex.exception))

    def test_shard_ids_valid_empty_shard_ids(self):
        self.config_input["shard_ids"] = []
        self.assertEqual([], kinesis.ClientConfig(self.config_input, self.boto_client).shard_ids)

    def test_shard_ids_valid_single_value(self):
        self.config_input["shard_ids"] = ["shardId-01"]
        self.assertEqual(["shardId-01"], kinesis.ClientConfig(self.config_input, self.boto_client).shard_ids)

    def test_shard_ids_valid_single_multi_value(self):
        self.config_input["shard_ids"] = ["shardId-01", "shardId-05"]
        self.assertEqual(["shardId-01", "shardId-05"],
                         kinesis.ClientConfig(self.config_input, self.boto_client).shard_ids)

    def test_poll_batch_size_invalid_string(self):
        self.config_input["poll_batch_size"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("If config-kinesis_scraper.yaml: \"poll_batch_size\" must be a positive numeric "
                      "string, or an integer.\nValue provided: <class 'str'> 'abc'", str(ex.exception))

    def test_poll_batch_size_invalid_over_max(self):
        self.config_input["poll_batch_size"] = 501
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("config-kinesis_scraper.yaml: poll_batch_size cannot exceed 500", str(ex.exception))

    def test_max_empty_polls_invalid_string(self):
        self.config_input["max_empty_polls"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("If config-kinesis_scraper.yaml: \"max_empty_polls\" must be a positive numeric "
                      "string, or an integer.\nValue provided: <class 'str'> 'abc'", str(ex.exception))

    def test_max_empty_polls_invalid_over_max(self):
        self.config_input["max_empty_polls"] = 2001
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("config-kinesis_scraper.yaml: max_empty_polls cannot exceed 2000", str(ex.exception))

    def test_total_records_per_shard_invalid_string(self):
        self.config_input["total_records_per_shard"] = "abc"
        self.config_input["ending_position"] = "TOTAL_RECORDS_PER_SHARD"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("If config-kinesis_scraper.yaml: \"total_records_per_shard\" must be a positive numeric "
                      "string, or an integer.\nValue provided: <class 'str'> 'abc'", str(ex.exception))

    def test_poll_delay_invalid_string(self):
        self.config_input["poll_delay"] = "abc"
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("If config-kinesis_scraper.yaml: \"poll_delay\" must be a positive numeric "
                      "string, a float, or an integer.\nValue provided: <class 'str'> 'abc'", str(ex.exception))

    def test_poll_delay_invalid_over_max(self):
        self.config_input["poll_delay"] = 2001
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertEqual("config-kinesis_scraper.yaml: poll_delay must be between 0-10", str(ex.exception))

    def test_poll_delay_invalid_under_max(self):
        self.config_input["poll_delay"] = -5
        with self.assertRaises(exceptions.ConfigValidationError) as ex:
            kinesis.ClientConfig(self.config_input, self.boto_client)
        self.assertIn("If config-kinesis_scraper.yaml: \"poll_delay\" must be a positive numeric "
                      "string, a float, or an integer.\nValue provided: <class 'int'> -5", str(ex.exception))
