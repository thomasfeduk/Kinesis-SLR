import boto3
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

log = logging.getLogger(__name__)


class ClientConfig(common.BaseCommonClass):
    def __init__(self, passed_data: [dict, str], boto_client: botocore.client.BaseClient):
        self._boto_client = boto_client
        self._debug_level = None
        self._stream_name = None
        self._shard_ids = None
        self._starting_position = None
        self._timestamp = None
        self._sequence_number = None
        self._poll_batch_size = None
        self._poll_delay = None
        self._max_total_records_per_shard = None
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
    def timestamp(self):
        return self._timestamp

    @property
    def sequence_number(self):
        return self._sequence_number

    @property
    def max_total_records_per_shard(self):
        return self._max_total_records_per_shard

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
        self._validate_iterator_types()
        self.validate_sequence_number(self._shard_ids, self._starting_position, self._sequence_number)
        self._validate_batch_size()
        self._validate_poll_delay()
        self._validate_max_total_records_per_shard()
        self._validate_max_empty_polls()

    def _validate_boto_client(self):
        if not isinstance(self.boto_client, botocore.client.BaseClient):
            raise TypeError(f"A boto3 Kinesis client object is required. Example: \"boto3.client('kinesis')\". "
                            f"Value provided: {str(type(self.boto_client))} {repr(self.boto_client)}")

    def _validate_batch_size(self):
        try:
            common.validate_numeric_pos(self._poll_batch_size)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"If config-kinesis_scraper.yaml: \"poll_batch_size\" must be a positive numeric "
                f"string, or an integer.\nValue provided: {repr(type(self._poll_batch_size))} "
            ) from e
        if int(self._poll_batch_size) > 500:
            raise ValueError('config-kinesis_scraper.yaml: poll_batch_size cannot exceed 500')

    def _validate_iterator_types(self):
        ClientConfig.validate_iterator_types(self._starting_position)
        if self._starting_position.upper() == 'AT_TIMESTAMP':
            try:
                common.validate_datetime(self._timestamp)
            except ValueError as e:
                raise ValueError(f"config-kinesis_scraper.yaml: Invalid format for config parameter \"timestamp\". "
                                 f"Format should be YYYY-MM-DD HH:MM:SS.\nValue provided: "
                                 f"{str(type(self._timestamp))} {repr(self._timestamp)}") from e

    def _validate_debug_level(self):
        debug_levels = [
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
        ]
        if self.debug_level not in debug_levels:
            raise ValueError('debug_level must be one of: ' + repr(debug_levels))

    def _validate_stream_name(self):
        if not isinstance(self._stream_name, str):
            raise TypeError(f"stream_name must be a string. Type provided: {str(type(self._stream_name))}")
        if self._stream_name == '' or self._stream_name == 'stream_name_here':
            raise ValueError('config-kinesis_scraper.yaml: A stream name must be set.')

    def _validate_required_configs(self):
        required_configs = [
            'debug_level',
            'stream_name',
            'shard_ids',
            'starting_position',
            # 'timestamp', # Conditionally required
            # 'sequence_number', # Conditionally required
            'max_total_records_per_shard',
            'poll_batch_size',
            'poll_delay',
            'max_empty_polls',
        ]
        for req_conf in required_configs:
            if getattr(self, req_conf) is None:
                raise ValueError(f"config-kinesis_scraper.yaml: Missing config parameter: {req_conf}")

    def _validate_max_empty_polls(self):
        try:
            common.validate_numeric_pos(self._max_empty_polls)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"If config-kinesis_scraper.yaml: \"max_empty_polls\" must be a positive numeric "
                f"string, or an integer.\nValue provided: {repr(type(self._max_empty_polls))} "
            ) from e
        if int(self._max_empty_polls) > 2000:
            raise ValueError('config-kinesis_scraper.yaml: max_empty_polls cannot exceed 2000')

    def _validate_max_total_records_per_shard(self):
        try:
            common.validate_numeric_pos(self._max_total_records_per_shard)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"If config-kinesis_scraper.yaml: \"max_total_records_per_shard\" must be a positive numeric "
                f"string, or an integer.\nValue provided: {repr(type(self._max_total_records_per_shard))}:"
                f" {repr(self._max_total_records_per_shard)}"
            ) from e

    def _validate_poll_delay(self):
        try:
            common.validate_numeric_pos(self.poll_delay)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"If config-kinesis_scraper.yaml: \"poll_delay\" must be either a numeric "
                f"string, a float, or an integer.\nValue provided: "
                f"{repr(type(self.poll_delay))} {repr(self.poll_delay)}"
            ) from e
        if float(self.poll_delay) < 0 or float(self.poll_delay) > 10:
            raise ValueError('config-kinesis_scraper.yaml: poll_delay must be between 0-10')

    def _post_init_processing(self):
        # Setup logging
        log.setLevel(self._debug_level)

        # If the starting position is not timestamp, clear the timestamp value, so we don't have it set internally
        # when we are never going to use it
        if self._starting_position.upper() != 'AT_TIMESTAMP':
            self._timestamp = None
        if self._poll_delay is not None:
            self._poll_delay = float(self._poll_delay)

    @staticmethod
    def validate_sequence_number(shard_ids: list, starting_position: str, sequence_number: [str, int]) -> None:
        ClientConfig.validate_iterator_types(starting_position)
        ClientConfig.validate_shard_ids(shard_ids)
        if starting_position in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']:
            # A sequence number is only unique within a shard, so we cannot specify at/after sequence
            # unless a single and only a single shard is id specified
            if len(shard_ids) != 1:
                raise ValueError(
                    f"If config-kinesis_scraper.yaml: \"starting_position\" is AT_SEQUENCE_NUMBER "
                    f"or AFTER_SEQUENCE_NUMBER, exactly 1 shard id must be specified as the sequence numbers "
                    f"are unique per shard.\nValue provided: {repr(type(shard_ids))} "
                )
            try:
                common.validate_numeric_pos(sequence_number)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"If config-kinesis_scraper.yaml: \"starting_position\" is AT_SEQUENCE_NUMBER "
                    f"or AFTER_SEQUENCE_NUMBER, the value must be a positive numeric string, float or an integer. "
                    f"\nValue provided: {repr(type(sequence_number))} "
                ) from e

    @staticmethod
    def validate_iterator_types(iterator_type: str) -> str:
        iterator_types = [
            "AT_SEQUENCE_NUMBER",
            "AFTER_SEQUENCE_NUMBER",
            "TRIM_HORIZON",
            "LATEST",
            "AT_TIMESTAMP"
        ]
        if iterator_type not in iterator_types:
            raise ValueError('iterator_type must be one of: ' + repr(iterator_types))
        return iterator_type

    @staticmethod
    def validate_shard_ids(shard_ids: list = None) -> list:
        # If we are dealing with a blank list of shard ids (None)
        # we want to proceed as that indicates we will be processing all shards
        if shard_ids is None:
            return []
        if not isinstance(shard_ids, list):
            raise TypeError(f'shard_ids must be of type list if specified. Type provided: {str(type(shard_ids))}')
        for shard_id in shard_ids:
            ClientConfig.validate_shard_id(shard_id)
        return shard_ids

    @staticmethod
    def validate_shard_id(shard_id: str = None) -> str:
        if not isinstance(shard_id, str):
            raise TypeError(f'Each shard_id must be a string. Value provided: {repr(type(shard_id))} {repr(shard_id)}')

        if shard_id.strip() == '':
            raise TypeError(f'Each shard_id must be a populated string. Value provided: {repr(shard_id)}')
        return shard_id


class Client:
    def __init__(self, client_config: ClientConfig):
        self._client_config = client_config
        self._is_valid()

        # Setup default attributes
        self._current_shard_iterator = None
        # How many events were fetched for the current shard
        self._total_records_fetched = 0

    def _is_valid(self):
        if not isinstance(self._client_config, ClientConfig):
            raise TypeError(f"client_config must be an instance of ClientConfig. Value provided: "
                            f"{repr(type(self._client_config))} {repr(self._client_config)}")

    def _confirm_shards_exist(self, shard_ids=None):
        # Because we may want to check the auto-detected shards, or a list of user-supplied shards
        # this method must be passed an explicit list as it could be called for either case
        if shard_ids is None:
            shard_ids = []

        for shard_id in shard_ids:
            iterator = self._get_new_shard_iterator(shard_id)
            pvd(iterator)
        pvdd('got here')

    def begin_scraping(self):
        shard_ids = self._get_shard_ids_of_stream()
        self._confirm_shards_exist(shard_ids)

    def _scrape_shards(self):
        shard_id = "1"
        # TODO Add a diretory exists check exception here before calling scrape shard method
        self._scrape_records_for_shard(shard_id)

    def _scrape_records_for_shard(self, shard_id: str) -> list:
        i = 1
        iterator = self._shard_iterator(shard_id)
        count_response_no_records = 0
        found_records = []
        while iterator:
            # TODO:  Add separate "Total found records" and "Total found records per shard" log info

            # Poll delay injection
            if self._client_config.poll_delay > 0:
                log.info(f"Wait delay of {self._client_config.poll_delay} seconds per poll_delay setting...")
                time.sleep(self._client_config.poll_delay)

            log.info(f'get_records() loop count: {str(i)} for shard: {shard_id}')
            log.debug(f'Current iterator: {iterator}')
            # Make the boto3 call

            try:
                records = self._client_config.boto_client.get_records(
                    ShardIterator=iterator,
                    Limit=self._client_config.poll_batch_size
                )
                response = records
            except Exception as ex:
                log.error(f'Received an error calling boto3 kinesis get_records()')
                log.error(ex)
                raise ex
            try:
                # Store next iterator for subsequent loops
                next_iterator = response['NextShardIterator']
            except KeyError as ex:
                log.error(f'received an unexpected response from boto3 kinesis get_records(): {repr(ex)}')
                log.debug(f'Response value:')
                log.debug(response)
                raise ex

            # Store records if found in temp list
            if len(response["Records"]) > 0:
                # Write the found records before any breaks occur
                self._process_records(shard_id, response["Records"])

                self._total_records_fetched += 1
                log.info(
                    f"\n\n{len(response['Records'])} records found in current get_records() response for shard:"
                    f" {shard_id}. Total found records: {len(found_records) + len(response['Records'])}\n")
                log.debug(response)
                count_response_no_records = 0

                # Append the records to found_records (upto N records, so we don't exceed max_total_records_per_shard)
                records_count_upto_to_add = self._client_config.max_total_records_per_shard - len(found_records)
                # If max_total_records_per_shard if 0, we include all records by passing 0 as the upto argument
                if self._client_config.max_total_records_per_shard == 0:
                    records_count_upto_to_add = 0
                common.list_append_upto_n_items(found_records, response["Records"], records_count_upto_to_add)

                if self._client_config.max_total_records_per_shard > 0 and \
                        0 <= self._client_config.max_total_records_per_shard <= len(found_records):
                    log.info(f'Reached {self._client_config.max_total_records_per_shard} max records per shard '
                             f'limit for shard {shard_id}\n')
                    break
            else:
                log.debug(response)
                count_response_no_records += 1
                log.info(f'No records found in loop. Currently at {count_response_no_records} empty calls, '
                         f'MillisBehindLatest: {response["MillisBehindLatest"]}.')

                if count_response_no_records > self._client_config.max_empty_polls - 1:
                    log.info(f'\n\nReached {self._client_config.max_empty_polls} empty polls for shard {shard_id} '
                             f'and found a total of {len(found_records)} records, '
                             f'current iterator: {iterator}\nAborting further reads for current shard.')
                    break

            # Update  iterator to next_iterator for subsequent loop
            iterator = next_iterator
            i += 1

        return found_records

    # If we don't have an existing/current shard iterator, we grab a new one, otherwise return the current one
    def _shard_iterator(self, shard_id: str) -> str:
        if not isinstance(shard_id, str):
            raise ValueError(f"shard_id must be a string.\nType provided: {repr(type(shard_id))}")

        if self._current_shard_iterator is not None:
            return self._current_shard_iterator

        return self._get_new_shard_iterator(shard_id)

    def _get_new_shard_iterator(self, shard_id: str) -> str:
        if not isinstance(shard_id, str):
            raise ValueError(f"shard_id must be a string.\nType provided: {repr(type(shard_id))}")

        # If we have a timestamp specified, we call client.get_shard_iterator with the timestamp,
        # otherwise call it without that argument
        log.info(f'Getting iterator for shard id: {shard_id}')
        if self._client_config.timestamp is None:
            response = self._client_config.boto_client.get_shard_iterator(
                StreamName=self._client_config.stream_name,
                ShardId=shard_id,
                ShardIteratorType=self._client_config.starting_position,
            )
        else:
            response = self._client_config.boto_client.get_shard_iterator(
                StreamName=self._client_config.stream_name,
                ShardId=shard_id,
                ShardIteratorType=self._client_config.starting_position,
                Timestamp=self._client_config.timestamp
            )

        try:
            iterator = response['ShardIterator']
        except KeyError as ex:
            log.error(f'received an unexpected response from boto3 kinesis get_shard_iterator(): {repr(ex)}')
            log.debug(f'Response value:')
            log.debug(response)
            raise ex
        log.debug('Returned Iterator: ' + iterator)
        return iterator

    def _get_shard_ids_of_stream(self) -> list:
        log.info('Getting shard ids from AWS...')
        response = self._client_config.boto_client.describe_stream(StreamName=self._client_config.stream_name)
        log.info(f"Stream name: {response['StreamDescription']['StreamName']}")
        log.info(f"Stream ARN: {response['StreamDescription']['StreamARN']}")
        shard_ids = []
        shard_details = response['StreamDescription']['Shards']
        for node in shard_details:
            try:
                log.info(f"Found shard id: {node['ShardId']}")
                shard_ids.append(node['ShardId'])
            except KeyError as ex:
                log.error(f'received an unexpected response from boto3 kinesis describe_stream(): {repr(ex)}')
                log.debug(f'Response value:')
                log.debug(response)
                raise ex
        return shard_ids

    @staticmethod
    def _process_records(shard_id: str, records: list):
        # Safety: We strip all but safe characters before creating any files/dirs
        shard_id = re.sub(r'[^A-Za-z0-9]', '', shard_id)
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
