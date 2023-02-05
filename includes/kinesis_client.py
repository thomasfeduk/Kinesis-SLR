from typing import Union
import boto3
import includes.exceptions as exceptions
import json
import time
import re
from includes.debug import pvdd, pvd, die
import random
import datetime
import includes.common as common
import logging
import botocore
import os
from abc import ABC, abstractmethod

log = logging.getLogger(__name__)


class GetRecordsIteration(ABC):
    @abstractmethod
    def __init__(self, *,
                 total_found_records: int,
                 response_no_records: int,
                 loop_count: int,
                 shard_id: str
                 ):
        self._total_found_records = total_found_records
        self._response_no_records = response_no_records
        self._loop_count = loop_count
        self._shard_id = shard_id
        self._proprules = common.PropRules()
        self._proprules.add_prop("total_found_records", types=[int], numeric_positive=True)
        self._proprules.add_prop("response_no_records", types=[int], numeric_positive=True)
        self._proprules.add_prop("loop_count", types=[int], numeric_positive=True)
        self._proprules.add_prop("shard_id", types=[str])

        # Make sure to call is valid after calling super init ONLY in child classes
        # self._is_valid()  -- Do call call this in the abstract class

    @property
    def total_found_records(self):
        return self._total_found_records

    @total_found_records.setter
    def total_found_records(self, value):
        self._total_found_records = value
        self._is_valid()

    @property
    def response_no_records(self):
        return self._response_no_records

    @response_no_records.setter
    def response_no_records(self, value):
        self._response_no_records = value
        self._is_valid()

    @property
    def loop_count(self):
        return self._loop_count

    @loop_count.setter
    def loop_count(self, value):
        self._loop_count = value
        self._is_valid()

    @property
    def shard_id(self):
        return self._shard_id

    @abstractmethod
    def _is_valid(self):
        self._is_valid_proprules()

    def _is_valid_proprules(self) -> None:
        attribs = {}
        for item in dir(self):
            if not callable(getattr(self, item)):
                attribs[item] = getattr(self, item)
        self._proprules.validate(attribs)


class GetRecordsIterationInput(GetRecordsIteration):
    def __init__(self, *,
                 total_found_records: int,
                 response_no_records: int,
                 loop_count: int,
                 shard_iterator: str,
                 shard_id: str,
                 ):
        # Set up subclass specific attributes before calling super init
        self._shard_iterator = shard_iterator

        super().__init__(
            total_found_records=total_found_records,
            response_no_records=response_no_records,
            loop_count=loop_count,
            shard_id=shard_id
        )

        # Inject proprules requirement into the proprules requirements list after inheriting from parent
        self._proprules.add_prop("shard_iterator", types=[str])

        # Recall the validation that is called in super().__init__ again after appending the new proprules
        self._is_valid()

    @property
    def total_found_records(self):
        return self._total_found_records

    @total_found_records.setter
    def total_found_records(self, value):
        self._total_found_records = value
        self._is_valid()

    @property
    def shard_iterator(self):
        return self._shard_iterator

    def _is_valid(self):
        super()._is_valid()


class GetRecordsIterationResponse(GetRecordsIteration):
    def __init__(self, *,
                 total_found_records: int,
                 found_records: int,
                 response_no_records: int,
                 loop_count: int,
                 next_shard_iterator: str,
                 shard_id: str,
                 break_iteration: bool,
                 ):
        # Set up subclass specific attributes before calling super init
        self._found_records = found_records
        self._next_shard_iterator = next_shard_iterator
        self._break_iteration = break_iteration
        self.init_count = 0

        super().__init__(
            total_found_records=total_found_records,
            response_no_records=response_no_records,
            loop_count=loop_count,
            shard_id=shard_id
        )

        # Inject proprules requirement into the proprules requirements list after inheriting from parent
        self._proprules.add_prop("break_iteration", types=[bool])
        self._proprules.add_prop("found_records", types=[int], numeric_positive=True)
        self._proprules.add_prop("next_shard_iterator", types=[str])

        # Recall the validation that is called in super().__init__ again after appending the new proprules
        self._is_valid()

    @property
    def total_found_records(self):
        return self._total_found_records

    @property
    def found_records(self):
        return self._found_records

    @property
    def next_shard_iterator(self):
        return self._next_shard_iterator

    @property
    def break_iteration(self):
        return self._break_iteration

    def _is_valid(self):
        super()._is_valid()
        if self.found_records > self.total_found_records:
            raise exceptions.InvalidArgumentException(f"Calculation fault: found_records ({self.found_records}) cannot"
                                                      f" exceed total_found_records ({self.total_found_records}).")


class Record(common.BaseCommonClass):
    def __init__(self, passed_data: [dict]):
        self._SequenceNumber = None
        self._ApproximateArrivalTimestamp = None
        self._Data = None
        self._PartitionKey = None
        self._proprules = common.PropRules()
        self._proprules.add_prop("SequenceNumber", types=[str])
        self._proprules.add_prop("PartitionKey", types=[str])
        self._proprules.add_prop("ApproximateArrivalTimestamp", types=[datetime.datetime, str])

        # Have to call parent after defining attributes
        super().__init__(passed_data)

    @property
    def SequenceNumber(self) -> str:
        return self._SequenceNumber

    @property
    def ApproximateArrivalTimestamp(self) -> datetime.datetime:
        return self._ApproximateArrivalTimestamp

    @property
    def Data(self):
        return self._Data

    @property
    def PartitionKey(self) -> str:
        return self._PartitionKey


class RecordsCollection(common.RestrictedCollection):
    @property
    def expected_type(self):
        return Record

    # Override get item so we can typehint the explicit type
    def __getitem__(self, index) -> Record:
        return self._items[int(index)]


class Boto3GetRecordsResponse(common.BaseCommonClass):
    def __init__(self, passed_data: [dict]):
        self._Records = None
        self._NextShardIterator = None
        self._MillisBehindLatest = None
        self._proprules = common.PropRules()
        self._proprules.add_prop("Records", types=[RecordsCollection])
        self._proprules.add_prop("NextShardIterator", types=[str])
        self._proprules.add_prop("MillisBehindLatest", types=[int])

        # If Records exists in the passed data, we re-pack it as RecordCollection of Record items
        if 'Records' in passed_data.keys():
            passed_data["Records"] = RecordsCollection([Record(i) for i in passed_data["Records"]])

        # Have to call parent after defining attributes other they are not populated
        super().__init__(passed_data)

    @property
    def Records(self) -> RecordsCollection:
        return self._Records

    @property
    def NextShardIterator(self) -> str:
        return self._NextShardIterator

    @property
    def MillisBehindLatest(self) -> int:
        return self._MillisBehindLatest


class ClientConfig(common.BaseCommonClass):
    def __init__(self, passed_data: Union[dict, str], boto_client: botocore.client.BaseClient):
        self._boto_client = boto_client
        self._debug_level = None
        self._stream_name = None
        self._shard_ids = None
        self._starting_position = None
        self._starting_timestamp = None
        self._starting_sequence_number = None
        self._ending_position = None
        self._ending_timestamp = None
        self._ending_sequence_number = None
        self._total_records_per_shard = None
        self._poll_batch_size = None
        self._poll_delay = None
        self._max_empty_polls = None

        # Have to call parent after defining attributes other they are not populated
        super().__init__(passed_data)

    @property
    def boto_client(self):
        return self._boto_client

    @property
    def debug_level(self):
        return self._debug_level

    @property
    def stream_name(self):
        return self._stream_name

    @property
    def shard_ids(self):
        return self._shard_ids

    @property
    def starting_position(self):
        return self._starting_position

    @property
    def starting_timestamp(self):
        return self._starting_timestamp

    @property
    def starting_sequence_number(self):
        return self._starting_sequence_number

    @property
    def ending_position(self):
        return self._ending_position

    @property
    def ending_timestamp(self):
        return self._ending_timestamp

    @property
    def ending_sequence_number(self):
        return self._ending_sequence_number

    @property
    def total_records_per_shard(self):
        return self._total_records_per_shard

    @property
    def poll_batch_size(self):
        return self._poll_batch_size

    @property
    def poll_delay(self):
        return self._poll_delay

    @property
    def max_empty_polls(self):
        return self._max_empty_polls

    def _is_valid(self):
        self._validate_boto_client()
        self._validate_required_configs()
        self._validate_stream_name()
        self._validate_debug_level()
        ClientConfig.validate_shard_ids(self._shard_ids)
        self._validate_starting_ending_position('starting')
        self._validate_sequence_number('starting')
        self._validate_timestamp_usage('starting')
        self._validate_starting_ending_position('ending')
        self._validate_sequence_number('ending')
        self._validate_timestamp_usage('ending')
        self._validate_batch_size()
        self._validate_poll_delay()
        self._validate_total_records_per_shard()
        self._validate_max_empty_polls()

    def _validate_boto_client(self):
        if not isinstance(self.boto_client, botocore.client.BaseClient):
            raise exceptions.InvalidArgumentException(
                f"A boto3 Kinesis client object is required. Example: \"boto3.client('kinesis')\". "
                f"Value provided: {str(type(self.boto_client))} {repr(self.boto_client)}")

    def _validate_batch_size(self):
        try:
            common.validate_numeric_pos(self._poll_batch_size)
        except (TypeError, ValueError) as e:
            raise exceptions.ConfigValidationError(
                f"If config-kinesis_scraper.yaml: \"poll_batch_size\" must be a positive numeric "
                f"string, or an integer.\nValue provided: "
                f"{repr(type(self._poll_batch_size))} {repr(self._poll_batch_size)}"
            ) from e
        if int(self._poll_batch_size) > 500:
            raise exceptions.ConfigValidationError('config-kinesis_scraper.yaml: poll_batch_size cannot exceed 500')

    def _validate_debug_level(self):
        debug_levels = [
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
        ]
        if self.debug_level not in debug_levels:
            raise exceptions.ConfigValidationError(
                f"config-kinesis_scraper.yaml: debug_level must be one of: {repr(debug_levels)}\nValue provided: "
                f"{repr(type(self.debug_level))} {repr(self.debug_level)}")

    def _validate_stream_name(self):
        if not isinstance(self._stream_name, str):
            raise exceptions.ConfigValidationError(
                f"stream_name must be a string. Type provided: {str(type(self._stream_name))}")
        if self._stream_name == '' or self._stream_name == 'stream_name_here':
            raise exceptions.ConfigValidationError('config-kinesis_scraper.yaml: A stream name must be set.')

    def _validate_required_configs(self):
        required_configs = [
            'debug_level',
            'stream_name',
            'shard_ids',
            'starting_position',
            # 'starting_timestamp',  # Conditionally required
            # 'starting_sequence_number',   # Conditionally required
            'ending_position',
            # 'ending_timestamp',  # Conditionally required
            # 'ending_sequence_number',  # Conditionally required
            # 'total_records_per_shard',  # Conditionally required
            'poll_batch_size',
            'poll_delay',
            'max_empty_polls',
        ]
        for req_conf in required_configs:
            if getattr(self, req_conf) is None:
                raise exceptions.ConfigValidationError(
                    f"config-kinesis_scraper.yaml: Missing config parameter: {req_conf}")

    def _validate_max_empty_polls(self):
        try:
            common.validate_numeric_pos(self._max_empty_polls)
        except (TypeError, ValueError) as e:
            raise exceptions.ConfigValidationError(
                f"If config-kinesis_scraper.yaml: \"max_empty_polls\" must be a positive numeric "
                f"string, or an integer.\nValue provided: "
                f"{repr(type(self._max_empty_polls))} {repr(self._max_empty_polls)}"
            ) from e
        if int(self._max_empty_polls) > 2000:
            raise exceptions.ConfigValidationError('config-kinesis_scraper.yaml: max_empty_polls cannot exceed 2000')

    def _validate_total_records_per_shard(self):
        if self.ending_position == 'TOTAL_RECORDS_PER_SHARD':
            try:
                common.validate_numeric_pos(self._total_records_per_shard)
            except (TypeError, ValueError) as e:
                raise exceptions.ConfigValidationError(
                    f"If config-kinesis_scraper.yaml: \"total_records_per_shard\" must be a positive numeric "
                    f"string, or an integer.\nValue provided: {repr(type(self._total_records_per_shard))} "
                    f"{repr(self._total_records_per_shard)}"
                ) from e

    def _validate_poll_delay(self):
        try:
            common.validate_numeric_pos(self.poll_delay)
        except (TypeError, ValueError) as e:
            raise exceptions.ConfigValidationError(
                f"If config-kinesis_scraper.yaml: \"poll_delay\" must be a positive numeric "
                f"string, a float, or an integer.\nValue provided: "
                f"{repr(type(self.poll_delay))} {repr(self.poll_delay)}"
            ) from e
        if float(self.poll_delay) < 0 or float(self.poll_delay) > 10:
            raise exceptions.ConfigValidationError('config-kinesis_scraper.yaml: poll_delay must be between 0-10')

    def _post_init_processing(self):
        super()._post_init_processing()
        # Setup logging
        log.setLevel(self._debug_level)

        # If the starting position is not timestamp, clear the timestamp value, so we don't have it set internally
        # when we are never going to use it. Same for sequence number
        if self._starting_position != 'AT_TIMESTAMP':
            self._starting_timestamp = None
        if self._starting_position not in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']:
            self._starting_sequence_number = None
        if self._ending_position not in ['AT_TIMESTAMP', 'BEFORE_TIMESTAMP', 'AFTER_TIMESTAMP']:
            self._ending_timestamp = None
        if self._ending_position not in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER', 'BEFORE_SEQUENCE_NUMBER']:
            self._ending_sequence_number = None
        if self._poll_delay is not None:
            self._poll_delay = float(self._poll_delay)

    def _validate_sequence_number(self, position_type: str) -> None:
        # If any of these are set for the {starting/ending}_positions, the shard_id must have exactly 1 value
        if getattr(self, f"{position_type}_position") in [
            'AT_SEQUENCE_NUMBER',
            'AFTER_SEQUENCE_NUMBER',
            'BEFORE_SEQUENCE_NUMBER',
        ]:
            # A sequence number is only unique within a shard, so we cannot specify at/after/before sequence
            # unless a single and only a single shard is id specified
            if len(self._shard_ids) != 1:
                value_provided_type = repr(type(self._shard_ids))
                value_provided = repr(self._shard_ids)
                raise exceptions.ConfigValidationError(
                    f"config-kinesis_scraper.yaml: If \"{position_type}_position\" is *_SEQUENCE_NUMBER, "
                    f"exactly 1 shard_id must be specified as the sequence numbers are unique per shard."
                    f"\nValue provided: {value_provided_type} {value_provided}"
                )
            try:
                common.validate_numeric_pos(getattr(self, f"{position_type}_sequence_number"))
            except (TypeError, ValueError) as e:
                value_provided_type = repr(type(getattr(self, f"{position_type}_sequence_number")))
                value_provided = repr(getattr(self, f"{position_type}_sequence_number"))
                raise exceptions.ConfigValidationError(
                    f"config-kinesis_scraper.yaml: If \"{position_type}_position\" is *_SEQUENCE_NUMBER, "
                    f"the value must be a positive numeric string, float or an integer."
                    f"\nValue provided: {value_provided_type} {value_provided}"
                ) from e

    def _validate_timestamp_usage(self, position_type: str) -> None:
        # If any of these are set for the {starting/ending}_positions, a valid timestamp is required
        if getattr(self, f"{position_type}_position") in [
            'AT_TIMESTAMP',
            'AFTER_TIMESTAMP',
            'BEFORE_TIMESTAMP',
        ]:

            timestamp = getattr(self, f"{position_type}_timestamp")
            try:
                common.validate_datetime(timestamp)
            except (ValueError, TypeError) as e:
                raise exceptions.ConfigValidationError(
                    f"config-kinesis_scraper.yaml: Invalid format for config parameter \"{position_type}_timestamp\".\n"
                    f"Format should be YYYY-MM-DD HH:MM:SS. "
                    f"Value provided: {str(type(timestamp))} {repr(timestamp)}") from e

    def _validate_starting_ending_position(self, position_type: str):
        positions = {
            "starting": [
                "AT_SEQUENCE_NUMBER",
                "AFTER_SEQUENCE_NUMBER",
                "TRIM_HORIZON",
                "LATEST",
                "AT_TIMESTAMP"
            ],
            "ending": [
                'TOTAL_RECORDS_PER_SHARD',
                'AT_SEQUENCE_NUMBER',
                'AFTER_SEQUENCE_NUMBER',
                'BEFORE_SEQUENCE_NUMBER',
                'AT_TIMESTAMP',
                'BEFORE_TIMESTAMP',
                'AFTER_TIMESTAMP',
                'LATEST',
            ]
        }

        valid_positions = positions[position_type]
        value_provided = getattr(self, f"{position_type}_position")
        if value_provided not in valid_positions:
            raise exceptions.ConfigValidationError(
                f"config-kinesis_scraper.yaml: {position_type}_position "
                f"must be one of: {repr(valid_positions)}\n"
                f"Value provided: {str(type(value_provided))} {repr(value_provided)}")

    @staticmethod
    def validate_shard_ids(shard_ids: list = None) -> list:
        # If we are dealing with a blank list of shard ids (None)
        # we want to proceed as that indicates we will be processing all shards
        if shard_ids is None:
            return []
        if not isinstance(shard_ids, list):
            raise exceptions.ConfigValidationError(f'shard_ids must be of type list if specified. Type provided: '
                                                   f'{str(type(shard_ids))} {repr(shard_ids)}')
        for shard_id in shard_ids:
            ClientConfig.validate_shard_id(shard_id)
        return shard_ids

    @staticmethod
    def validate_shard_id(shard_id: str = None) -> str:
        if not isinstance(shard_id, str):
            raise exceptions.ConfigValidationError(f'Each shard_id must be a string. '
                                                   f'Value provided: {repr(type(shard_id))} {repr(shard_id)}')

        if shard_id.strip() == '':
            raise exceptions.ConfigValidationError(f'Each shard_id must be a populated string. '
                                                   f'Value provided: {repr(type(shard_id))} {repr(shard_id)}')
        return shard_id


class Client:
    def __init__(self, client_config: ClientConfig):
        self._client_config = client_config
        self._is_valid()

        # Setup default attributes
        self._current_shard_iterator = None

    def _is_valid(self):
        if not isinstance(self._client_config, ClientConfig):
            raise exceptions.ConfigValidationError(
                f"client_config must be an instance of ClientConfig. "
                f"Value provided: {repr(type(self._client_config))} {repr(self._client_config)}")

    def _confirm_shards_exist(self, shard_ids_detected: list):
        for shard_id in self._client_config.shard_ids:
            if shard_id not in shard_ids_detected:
                raise exceptions.ConfigValidationError(
                    f'Specified shard_id "{shard_id}" does not exist in stream '
                    f'"{self._client_config.stream_name}". Detected shards: {repr(shard_ids_detected)}')

    def begin_scraping(self):
        shard_ids_detected = self._get_shard_ids_of_stream()
        self._confirm_shards_exist(shard_ids_detected)

        # Pre-check: Confirm we are not trying to scrape any shards that already have been written to disk
        log.debug('Checking for exiting shard directories before beginning scraping')
        for shard_id in shard_ids_detected:
            dir_path = f'scraped_events/{shard_id}'
            log.debug(dir_path)
            log.debug(f'Checking if shard dir exists {dir_path}: {os.path.exists(dir_path)}')
            if os.path.exists(dir_path):
                raise FileExistsError(f'Scrapped shard directory {dir_path} currently exists. To prevent '
                                      f'conflicts/overwriting existing data, please move any previously scrapped '
                                      f'messages and their shard id directory elsewhere and re-run this tool. ')

        # Passed all checks, now we iterate through each shard id specified by the config (or all if not specified)
        shard_ids_to_scrape = shard_ids_detected
        if len(self._client_config.shard_ids) > 0:
            shard_ids_to_scrape = self._client_config.shard_ids

        log.debug(f'Shard ids to scrape: {repr(shard_ids_to_scrape)}')
        self._scrape_shards(shard_ids_to_scrape)

    def _scrape_shards(self, shard_ids=None):
        if shard_ids is None:
            raise exceptions.InvalidArgumentException('_scrape_shards called with None.')

        for shard_id in shard_ids:
            self._scrape_records_for_shard(shard_id)

    def _scrape_records_for_shard(self, shard_id: str) -> None:
        next_shard_iterator = self._shard_iterator(shard_id)

        total_found_records = 0
        response_no_records = 0
        loop_count = 1

        while next_shard_iterator:
            iterator_response_obj = self._scrape_records_for_shard_iterator(GetRecordsIterationInput(
                total_found_records=total_found_records,
                response_no_records=response_no_records,
                shard_iterator=next_shard_iterator,
                loop_count=loop_count,
                shard_id=shard_id
            )
            )

            # Break the iteration only if the iteration response states it is time to do so
            if iterator_response_obj.break_iteration:
                break

            # Set the variables for the next iteration
            total_found_records = iterator_response_obj.total_found_records
            response_no_records = iterator_response_obj.response_no_records
            next_shard_iterator = iterator_response_obj.next_shard_iterator
            loop_count = iterator_response_obj.loop_count

    def _scrape_records_for_shard_iterator(self, iterator_obj: GetRecordsIterationInput) \
            -> GetRecordsIterationResponse:

        # Increment total loop counter
        iterator_obj.loop_count += 1

        self._scrape_records_for_shard_handle_poll_delay(
            iterator_obj.loop_count, iterator_obj.shard_iterator, iterator_obj.shard_id
        )

        # Make the boto3 call
        response = self._get_records(iterator_obj.shard_iterator)
        # if len(response.Records) > 0:
        #     # pvdd(response.Records[0].SequenceNumber)
        #     # pvdd(json.dumps(response.Records, default=str, indent=4))
        #     # die('got here 649')
        #

        # Store records if found in temp list
        if len(response.Records) > 0:
            log.debug(f'Found {len(response.Records)} in batch.')

            iterator_obj.total_found_records += 1
            log.info(
                f"\n\n{len(response.Records)} records found in current get_records() response for shard:"
                f" {iterator_obj.shard_id}. Total found records: "
                f"{iterator_obj.total_found_records + len(response.Records)}\n")
            iterator_obj.response_no_records = 0

            # If ending_positoin is total records per shard, append upto X records to found_records
            records_count_upto_to_add = 0
            records_to_process = []

            pvdd(iterator_obj.total_found_records)
            if self._client_config.ending_position == 'TOTAL_RECORDS_PER_SHARD':
                records_count_upto_to_add = self._client_config.total_records_per_shard - len(response.Records)
                # If total_records_per_shard if 0, we include all records by passing 0 as the upto argument

            pvd(f"total per shard: {self._client_config.total_records_per_shard}")
            pvd(f"respone.Records: {len(response.Records)}")
            pvd(records_count_upto_to_add)
            die('sadsadsd')

            output = common.list_append_upto_n_items(
                records_to_process,
                [i for i in response.Records],
                records_count_upto_to_add)

            pvdd(output)

            self._process_records(iterator_obj.shard_id,
                                  RecordsCollection(common.list_append_upto_n_items(
                                      records_to_process,
                                      [i for i in response.Records],
                                      records_count_upto_to_add)
                                  )
                                  )
            die('kinesis client line 540')

            # If we are at the total per shard, we terminate the loop
            if self._client_config.total_records_per_shard > 0 and \
                    0 < self._client_config.total_records_per_shard <= len(iterator_obj):
                log.info(f'Reached {self._client_config.total_records_per_shard} max records per shard '
                         f'limit for shard {iterator_obj.shard_id}\n')
                # break
            # Append the new records for this iteration to the found records var
            found_records = common.list_append_upto_n_items(
                found_records, response["Records"], records_count_upto_to_add)

        else:
            log.debug(response)
            iterator_obj.response_no_records += 1
            log.info(f'No records found in loop. Currently at {iterator_obj.response_no_records} empty calls, '
                     f'MillisBehindLatest: {response.MillisBehindLatest}.')

            if iterator_obj.response_no_records > self._client_config.max_empty_polls - 1:
                log.info(f'\n\nReached {self._client_config.max_empty_polls} empty polls for shard '
                         f'{iterator_obj.shard_id} and found a total of {len(response.Records)} records, '
                         f'current iterator: {iterator_obj.shard_iterator}\n'
                         f'Aborting further reads for current shard: {iterator_obj.shard_id}')
                return GetRecordsIterationResponse(
                    total_found_records=iterator_obj.total_found_records,
                    response_no_records=iterator_obj.response_no_records,
                    loop_count=iterator_obj.loop_count,
                    next_shard_iterator=response.NextShardIterator,
                    shard_id=iterator_obj.shard_id,
                    found_records=len(response.Records),
                    break_iteration=True,
                )

        # End of iteration, build and return new iterator response
        return GetRecordsIterationResponse(
            total_found_records=iterator_obj.total_found_records,
            response_no_records=iterator_obj.response_no_records,
            loop_count=iterator_obj.loop_count,
            next_shard_iterator=response.NextShardIterator,
            shard_id=iterator_obj.shard_id,
            found_records=len(response.Records),
            break_iteration=False,
        )
        # return found_records, response_no_records, iterator

    def _scrape_records_for_shard_handle_poll_delay(self, loop_count: int, iterator: str, shard_id: str) -> None:
        if self._client_config.poll_delay > 0:
            log.info(f"Wait delay of {self._client_config.poll_delay} seconds per poll_delay setting...")
            time.sleep(self._client_config.poll_delay)
        log.info(f'get_records() loop count: {str(loop_count)} for shard: {shard_id}')
        log.debug(f'Current iterator: {iterator}')

    def _get_records(self, iterator: str) -> Boto3GetRecordsResponse:
        timer_start = time.time()
        response = self._client_config.boto_client.get_records(
            ShardIterator=iterator,
            Limit=self._client_config.poll_batch_size
        )
        timer_end = time.time()
        log.debug(f'get_records() completed in {timer_end - timer_start} seconds.')

        return Boto3GetRecordsResponse(response)

    # If we don't have an existing/current shard iterator, we grab a new one, otherwise return the current one
    def _shard_iterator(self, shard_id: str) -> str:
        if not isinstance(shard_id, str):
            raise exceptions.InvalidArgumentException(
                f"shard_id must be a string.\nType provided: {repr(type(shard_id))}")

        if self._current_shard_iterator is not None:
            return self._current_shard_iterator

        return self._get_new_shard_iterator(shard_id)

    def _get_new_shard_iterator(self, shard_id: str) -> str:
        if not isinstance(shard_id, str):
            raise exceptions.InvalidArgumentException(
                f"shard_id must be a string.\nType provided: {repr(type(shard_id))}")

        # If we have a timestamp specified, we call client.get_shard_iterator with the timestamp,
        # otherwise call it without that argument
        log.info(f'Getting iterator for shard id: {shard_id}')
        if self._client_config.starting_timestamp is not None:
            log.debug(f'Calling get_shard_iterator() with: '
                      f'StreamName={self._client_config.stream_name} '
                      f'ShardId={shard_id} '
                      f'ShardIteratorType={self._client_config.starting_position} '
                      f'Timestamp={self._client_config.starting_timestamp}'
                      )
            response = self._client_config.boto_client.get_shard_iterator(
                StreamName=self._client_config.stream_name,
                ShardId=shard_id,
                ShardIteratorType=self._client_config.starting_position,
                Timestamp=self._client_config.starting_timestamp
            )
        elif self._client_config.starting_sequence_number is not None:
            log.debug(f'Calling get_shard_iterator() with: '
                      f'StreamName={self._client_config.stream_name} '
                      f'ShardId={shard_id} '
                      f'ShardIteratorType={self._client_config.starting_position} '
                      f'StartingSequenceNumber={self._client_config.starting_sequence_number}'
                      )
            response = self._client_config.boto_client.get_shard_iterator(
                StreamName=self._client_config.stream_name,
                ShardId=shard_id,
                ShardIteratorType=self._client_config.starting_position,
                StartingSequenceNumber=self._client_config.starting_sequence_number,
            )
        else:
            log.debug(f'Calling get_shard_iterator() with: '
                      f'StreamName={self._client_config.stream_name} '
                      f'ShardId={shard_id} '
                      f'ShardIteratorType={self._client_config.starting_position}'
                      )
            response = self._client_config.boto_client.get_shard_iterator(
                StreamName=self._client_config.stream_name,
                ShardId=shard_id,
                ShardIteratorType=self._client_config.starting_position,
            )

        try:
            iterator = response['ShardIterator']
        except Exception as ex:
            error_msg = f'received an unexpected response from boto3 kinesis get_shard_iterator(): {repr(ex)}'
            log.error(error_msg)
            log.debug(f'Response value:')
            log.debug(response)
            raise exceptions.AwsUnexpectedResponse(error_msg) from ex
        log.debug('Returned Iterator: ' + iterator)
        return iterator

    def _get_shard_ids_of_stream(self) -> list:
        log.info('Getting shard ids from AWS...')
        response = self._client_config.boto_client.describe_stream(StreamName=self._client_config.stream_name)
        log.info(f"Stream name: {response['StreamDescription']['StreamName']}")
        log.info(f"Stream ARN: {response['StreamDescription']['StreamARN']}")

        shard_ids = []
        shard_details = response['StreamDescription']['Shards']
        log.debug(f'Detecting shard ids that exist for stream: {self._client_config.stream_name}')

        for node in shard_details:
            try:
                log.info(f"Detected shard id: {node['ShardId']}")
                shard_ids.append(node['ShardId'])
            except Exception as ex:
                error_msg = f'received an unexpected response from boto3 kinesis describe_stream(): {repr(ex)}'
                log.error(error_msg)
                log.debug(f'Response value:')
                log.debug(response)
                raise exceptions.AwsUnexpectedResponse(error_msg) from ex
        return shard_ids

    @staticmethod
    def _process_records(shard_id: str, records: RecordsCollection):
        if not isinstance(shard_id, str):
            raise exceptions.InvalidArgumentException(
                f'"shard_id" must be of type RecordsCollection. Received: {repr(type(shard_id))} {repr(shard_id)}')

        if not isinstance(records, RecordsCollection):
            raise exceptions.InvalidArgumentException(
                f'"records" must be of type RecordsCollection. Received: {repr(type(records))} {repr(records)}')

        # Safety: We strip all but safe characters before creating any files/dirs
        shard_id = re.sub(r'[^A-Za-z0-9-_]', '', shard_id)
        dir_path = f'scraped_events/{shard_id}'
        log.debug(f'Processing records for batch')
        log.debug(f'mkdirs path: {dir_path}')

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        prefix = common.count_files_in_dir(dir_path)
        log.debug(f'Initial prefix is: {prefix}')
        for record in records:
            prefix += 1
            # Safety: We strip all but safe characters before creating any files/dirs
            timestamp = re.sub(r'[^A-Za-z0-9-:_]', '',
                               record["ApproximateArrivalTimestamp"].strftime('%Y-%m-%d_%H:%M:%S'))
            log.debug(f'timestamp: {timestamp}')
            filename_uri = f"{dir_path}/{prefix}-{timestamp.replace(':', ';')}.json"
            log.debug(f'Filename: {filename_uri}')

            try:
                f = open(filename_uri, "x")
            except FileExistsError as ex:
                raise FileExistsError(f'The file "{filename_uri}" already exists when trying to create an event '
                                      f'record file. Be sure scraping is not being run with a populated '
                                      f'scraped_events/{shard_id} directory.') from ex
            f.write(json.dumps(record, default=str, indent=4))
            f.close()
