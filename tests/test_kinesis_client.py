from typing import Union
import uuid
import datetime
import json
import importlib
import unittest
import unittest.mock as mock
from unittest.mock import patch
import includes.kinesis_client as kinesis
import includes.exceptions as exceptions
import sys
from includes.debug import *
import includes.common as common


def generate_records(num: int) -> list:
    records = []
    for item in range(num):
        records.append(generate_record_obj())
    return records


def generate_record_raw_dict(*,
                             sequence_number: str = uuid.uuid4().hex,
                             timestamp: Union[str, datetime.datetime] = datetime.datetime.now(),
                             data="dataHere",
                             pkey: str = 'sample_event',
                             ) -> dict:
    record = {
        "SequenceNumber": sequence_number,
        "ApproximateArrivalTimestamp": timestamp,
        "Data": data,
        "PartitionKey": pkey
    }
    return record


def generate_record_obj(record_raw_dict=None) -> kinesis.Record:
    if record_raw_dict is None:
        record_raw_dict = generate_record_raw_dict()

    return kinesis.Record(record_raw_dict)


class TestRecord(unittest.TestCase):
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

    def test_record_invalid_empty(self):
        record_raw = {}
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.Record(record_raw)
        self.assertIn(
            "\"SequenceNumber\" attribute must be of type: [<class 'str'>]\n"
            "Received: <class 'NoneType'> None",
            str(ex.exception)
        )

    def test_record_invalid_type_SequenceNumber(self):
        record_raw = generate_record_raw_dict()
        # Cant have an int for sequence number
        record_raw["SequenceNumber"] = 5
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.Record(record_raw)
        self.assertIn(
            "\"SequenceNumber\" attribute must be of type: [<class 'str'>]\nReceived: <class 'int'> 5",
            str(ex.exception)
        )

    def test_record_invalid_type_PartitionKey(self):
        record_raw = generate_record_raw_dict()
        # Cant have an int for sequence number
        record_raw["PartitionKey"] = 1
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.Record(record_raw)
        self.assertIn(
            "\"PartitionKey\" attribute must be of type: [<class 'str'>]\nReceived: <class 'int'> 1",
            str(ex.exception)
        )

    def test_record_invalid_type_timestamp(self):
        record_raw = generate_record_raw_dict()
        # Cant have an int for sequence number
        record_raw["ApproximateArrivalTimestamp"] = []
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.Record(record_raw)
        self.assertIn(
            "\"ApproximateArrivalTimestamp\" attribute must be of type: "
            "[<class 'datetime.datetime'>, <class 'str'>]\nReceived: <class 'list'> []",
            str(ex.exception)
        )

    def test_record_valid_timestamp_datetime_obj(self):
        timestamp = datetime.datetime.now()
        record_raw = generate_record_raw_dict(sequence_number="1", timestamp=timestamp)
        record_obj = kinesis.Record(record_raw)
        self.assertEqual(record_obj.SequenceNumber, "1")
        self.assertEqual(record_obj.ApproximateArrivalTimestamp, timestamp)
        self.assertEqual(record_obj.PartitionKey, "sample_event")
        self.assertEqual(record_obj.Data, "dataHere")

    def test_record_valid_timestamp_string(self):
        timestamp = datetime.datetime.now()
        record_raw = generate_record_raw_dict(sequence_number="1", timestamp=timestamp.isoformat())
        record_obj = kinesis.Record(record_raw)
        self.assertEqual(record_obj.SequenceNumber, "1")
        self.assertEqual(record_obj.ApproximateArrivalTimestamp, timestamp.isoformat())
        self.assertEqual(record_obj.PartitionKey, "sample_event")
        self.assertEqual(record_obj.Data, "dataHere")


class TestGetRecordsIterationInput(unittest.TestCase):
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

    def test_invalid_type_total_found_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationInput(
                total_found_records="10",
                response_no_records=0,
                loop_count=15,
                shard_iterator="abc",
                shard_id="shard-123"
            )

        self.assertIn(
            "\"total_found_records\" attribute must be of type: [<class 'int'>]\n"
            "Received: <class 'str'> '10'",
            str(ex.exception)
        )

    def test_invalid_type_response_no_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationInput(
                total_found_records=10,
                response_no_records='blah',
                loop_count=15,
                shard_iterator="abc",
                shard_id="shard-123"
            )

        self.assertIn(
            "\"response_no_records\" attribute must be of type: [<class 'int'>]\n"
            "Received: <class 'str'> 'blah'",
            str(ex.exception)
        )

    def test_invalid_type_loop_count(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationInput(
                total_found_records=10,
                response_no_records=0,
                loop_count="15",
                shard_iterator="abc",
                shard_id="shard-123"
            )

        self.assertIn(
            "\"loop_count\" attribute must be of type: [<class 'int'>]\n"
            "Received: <class 'str'> '15'",
            str(ex.exception)
        )

    def test_invalid_type_shard_iterator(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationInput(
                total_found_records=10,
                response_no_records=0,
                loop_count=15,
                shard_iterator=None,
                shard_id="shard-123"
            )

        self.assertIn(
            "\"shard_iterator\" attribute must be of type: [<class 'str'>]\n"
            "Received: <class 'NoneType'> None",
            str(ex.exception)
        )

    def test_invalid_type_shard_id(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationInput(
                total_found_records=10,
                response_no_records=0,
                loop_count=15,
                shard_iterator="abc",
                shard_id=500,
            )

        self.assertIn(
            "\"shard_id\" attribute must be of type: [<class 'str'>]\n"
            "Received: <class 'int'> 500",
            str(ex.exception)
        )

    def test_invalid_negative_numeric_total_found_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationInput(
                total_found_records=-10,
                response_no_records=0,
                loop_count=15,
                shard_iterator="abc",
                shard_id="shard-500",
            )

        self.assertIn("\"total_found_records\" must be a positive numeric value. Received: <class 'int'> -10",
                      str(ex.exception))

    def test_invalid_negative_numeric_response_no_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationInput(
                total_found_records=10,
                response_no_records=-2,
                loop_count=15,
                shard_iterator="abc",
                shard_id="shard-500",
            )

        self.assertIn("\"response_no_records\" must be a positive numeric value. Received: <class 'int'> -2",
                      str(ex.exception))

    def test_invalid_negative_numeric_loop_count(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationInput(
                total_found_records=10,
                response_no_records=2,
                loop_count=-15,
                shard_iterator="abc",
                shard_id="shard-500",
            )

        self.assertIn("\"loop_count\" must be a positive numeric value. Received: <class 'int'> -15",
                      str(ex.exception))

    def test_valid(self):
        iteration_input = kinesis.GetRecordsIterationInput(
            total_found_records=10,
            response_no_records=0,
            loop_count=15,
            shard_iterator="abc",
            shard_id="shard-123"
        )

        self.assertEqual(iteration_input.total_found_records, 10)
        self.assertEqual(iteration_input.response_no_records, 0)
        self.assertEqual(iteration_input.loop_count, 15)
        self.assertEqual(iteration_input.shard_iterator, "abc")
        self.assertEqual(iteration_input.shard_id, "shard-123")


class TestGetRecordsIterationOutput(unittest.TestCase):
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

    def test_invalid_type_total_found_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records="10",
                found_records=50,
                response_no_records=2,
                loop_count=15,
                next_shard_iterator="abc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn(
            "\"total_found_records\" attribute must be of type: [<class 'int'>]\n"
            "Received: <class 'str'> '10'",
            str(ex.exception)
        )

    def test_invalid_type_found_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records="50",
                response_no_records=2,
                loop_count=15,
                next_shard_iterator="abc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn(
            "\"found_records\" attribute must be of type: [<class 'int'>]\n"
            "Received: <class 'str'> '50'",
            str(ex.exception)
        )

    def test_invalid_type_response_no_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=50,
                response_no_records="2",
                loop_count=15,
                next_shard_iterator="abc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn(
            "\"response_no_records\" attribute must be of type: [<class 'int'>]\n"
            "Received: <class 'str'> '2'",
            str(ex.exception)
        )

    def test_invalid_type_loop_count(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=50,
                response_no_records=2,
                loop_count="15",
                next_shard_iterator="abc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn(
            "\"loop_count\" attribute must be of type: [<class 'int'>]\n"
            "Received: <class 'str'> '15'",
            str(ex.exception)
        )

    def test_invalid_type_next_shard_iterator(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=50,
                response_no_records=2,
                loop_count=15,
                next_shard_iterator=5,
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn(
            "\"next_shard_iterator\" attribute must be of type: [<class 'str'>]\n"
            "Received: <class 'int'> 5",
            str(ex.exception)
        )

    def test_invalid_type_next_shard_id(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=50,
                response_no_records=2,
                loop_count=15,
                next_shard_iterator="abcc",
                shard_id=123,
                break_iteration=True
            )

        self.assertIn(
            "\"shard_id\" attribute must be of type: [<class 'str'>]\n"
            "Received: <class 'int'> 123",
            str(ex.exception)
        )

    def test_invalid_type_break_iteration(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=50,
                response_no_records=2,
                loop_count=15,
                next_shard_iterator="abcc",
                shard_id="shard-123",
                break_iteration="True"
            )

        self.assertIn(
            "\"break_iteration\" attribute must be of type: [<class 'bool'>]\n"
            "Received: <class 'str'> 'True'",
            str(ex.exception)
        )

    def test_invalid_negative_numeric_total_found_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=-10,
                found_records=50,
                response_no_records=2,
                loop_count=15,
                next_shard_iterator="abcc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn("\"total_found_records\" must be a positive numeric value. Received: <class 'int'> -10",
                      str(ex.exception))

    def test_invalid_negative_numeric_found_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=-50,
                response_no_records=2,
                loop_count=15,
                next_shard_iterator="abcc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn("\"found_records\" must be a positive numeric value. Received: <class 'int'> -50",
                      str(ex.exception))

    def test_invalid_negative_numeric_response_no_records(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=50,
                response_no_records=-2,
                loop_count=15,
                next_shard_iterator="abcc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn("\"response_no_records\" must be a positive numeric value. Received: <class 'int'> -2",
                      str(ex.exception))

    def test_invalid_negative_numeric_loop_count(self):
        with self.assertRaises(exceptions.InvalidArgumentException) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=50,
                response_no_records=2,
                loop_count=-15,
                next_shard_iterator="abcc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn("\"loop_count\" must be a positive numeric value. Received: <class 'int'> -15",
                      str(ex.exception))

    def test_invalid_total_found_less_than_found(self):
        with self.assertRaises(exceptions.InternalError) as ex:
            kinesis.GetRecordsIterationResponse(
                total_found_records=10,
                found_records=50,
                response_no_records=2,
                loop_count=15,
                next_shard_iterator="abcc",
                shard_id="shard-123",
                break_iteration=True
            )

        self.assertIn("Calculation fault: found_records (50) cannot exceed total_found_records (10).",
                      str(ex.exception))

    def test_valid(self):
        iteration_input = kinesis.GetRecordsIterationResponse(
            total_found_records=10,
            found_records=5,
            response_no_records=2,
            loop_count=15,
            next_shard_iterator="abc",
            shard_id="shard-123",
            break_iteration=True
        )

        self.assertEqual(iteration_input.total_found_records, 10)
        self.assertEqual(iteration_input.found_records, 5)
        self.assertEqual(iteration_input.response_no_records, 2)
        self.assertEqual(iteration_input.loop_count, 15)
        self.assertEqual(iteration_input.next_shard_iterator, "abc")
        self.assertEqual(iteration_input.shard_id, "shard-123")
        self.assertEqual(iteration_input.break_iteration, True)


class TestClientFullCycle(unittest.TestCase):
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

        # @patch('includes.kinesis_client.Client._get_records', create=True)
        # @patch('includes.kinesis_client.Client._shard_iterator', create=True)
        # @patch('os.path.exists', create=True)
        # @patch('includes.kinesis_client.Client._confirm_shards_exist', create=True)
        # @patch('includes.kinesis_client.Client._get_shard_ids_of_stream', create=True)
        # @patch('includes.kinesis_client.Client._scrape_records_for_shard_handle_poll_delay', create=True)
        # @patch('includes.kinesis_client.Client._is_valid', create=True)
        # def test_end_to_end_found_records(self,
        #                                   mocked_is_valid,
        #                                   mocked_poll,
        #                                   mocked_get_shard_ids_of_stream,
        #                                   mocked_confirm_shards_exist,
        #                                   mocked_os_path_exists,
        #                                   mocked_shard_iterator,
        #                                   mocked_get_records,
        #                                   ):
        #     mocked_get_shard_ids_of_stream.return_value = ['shard_test']
        #     mocked_os_path_exists.return_value = False
        #     mocked_shard_iterator.return_value = 'the_iter_id'
        #     mocked_get_records.return_value = kinesis.Boto3GetRecordsResponse({
        #         "Records": generate_records(10), "NextShardIterator": uuid.uuid4().hex, "MillisBehindLatest": 0
        #     })
        #
        #     config = mock.Mock()
        #     config.shard_ids = []
        #
        #     client = kinesis.Client(config)
        #
        #     client.begin_scraping()
        #     die('est here 40')

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
