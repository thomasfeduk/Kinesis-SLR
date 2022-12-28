import json
import importlib
import unittest
import unittest.mock as mock
from unittest.mock import patch
import includes.kinesis_client as kinesis
import sys
from includes.debug import *
import includes.common as common


class ClientConfigReqParams(unittest.TestCase):
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

    def test_stream_name(self):
        yaml_input = {

        }
        with self.assertRaises(ValueError) as ex:
            kinesis.ConfigClient(yaml_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: stream_name',
            str(ex.exception)
        )

    def test_shardIds(self):
        yaml_input = {
            "stream_name": "x"
        }
        with self.assertRaises(ValueError) as ex:
            kinesis.ConfigClient(yaml_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: shardIds',
            str(ex.exception)
        )

    def test_starting_position(self):
        yaml_input = {
            "stream_name": "x",
            "shardIds": [],
        }
        with self.assertRaises(ValueError) as ex:
            kinesis.ConfigClient(yaml_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: starting_position',
            str(ex.exception)
        )

    def test_max_total_records_per_shard(self):
        yaml_input = {
            "stream_name": "x",
            "shardIds": [],
            "starting_position": "x",
        }
        with self.assertRaises(ValueError) as ex:
            kinesis.ConfigClient(yaml_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: max_total_records_per_shard',
            str(ex.exception)
        )

    def test_poll_batch_size(self):
        yaml_input = {
            "stream_name": "x",
            "shardIds": [],
            "starting_position": "x",
            "max_total_records_per_shard": "x",

        }
        with self.assertRaises(ValueError) as ex:
            kinesis.ConfigClient(yaml_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: poll_batch_size',
            str(ex.exception)
        )

    def test_poll_delay(self):
        yaml_input = {
            "stream_name": "x",
            "shardIds": [],
            "starting_position": "x",
            "max_total_records_per_shard": "x",
            "poll_batch_size": "x"

        }
        with self.assertRaises(ValueError) as ex:
            kinesis.ConfigClient(yaml_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: poll_delay',
            str(ex.exception)
        )

    def test_max_empty_record_polls(self):
        yaml_input = {
            "stream_name": "x",
            "shardIds": [],
            "starting_position": "x",
            "max_total_records_per_shard": "x",
            "poll_batch_size": "x",
            "poll_delay": "x"

        }
        with self.assertRaises(ValueError) as ex:
            kinesis.ConfigClient(yaml_input)
        self.assertEqual(
            'config-kinesis_scraper.yaml: Missing config parameter: max_empty_record_polls',
            str(ex.exception)
        )

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
