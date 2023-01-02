import boto3
import logging
import json
import time
from includes.debug import pvdd, pvd, die
import random
import datetime
import includes.common as common
import logging
import botocore

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class ClientConfig(common.ConfigSLR):
    def __init__(self, passed_data: [dict, str], boto_client: botocore.client.BaseClient):
        self._boto_client = boto_client
        self._stream_name = None
        self._shard_ids = None
        self._starting_position = None
        self._timestamp = None
        self._sequence_number = None
        self._max_total_records_per_shard = None
        self._poll_batch_size = None
        self._poll_delay = None
        self._max_empty_record_polls = None

        # Have to call parent after defining attributes other they are not populated
        super().__init__(passed_data)

    @property
    def boto_client(self):
        return self._boto_client

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
    def max_empty_record_polls(self):
        return self._max_empty_record_polls

    def _is_valid(self):
        # Confirm we have received a real boto client object instance
        if not isinstance(self.boto_client, botocore.client.BaseClient):
            raise TypeError(f"A boto3 Kinesis client object is required. Example: \"boto3.client('kinesis')\". "
                            f"Value provided: {str(type(self.boto_client))} {repr(self.boto_client)}")

        # Confirm minimum needed passed_data values exist
        required_configs = [
            'stream_name',
            'shard_ids',
            'starting_position',
            # 'timestamp', # Conditionally required
            # 'sequence_number', # Conditionally required
            'max_total_records_per_shard',
            'poll_batch_size',
            'poll_delay',
            'max_empty_record_polls',
        ]

        for req_conf in required_configs:
            if getattr(self, req_conf) is None:
                raise ValueError(f"config-kinesis_scraper.yaml: Missing config parameter: {req_conf}")
        # Stream Name
        if not isinstance(self._stream_name, str):
            raise TypeError(f"stream_name must be a string. Type provided: {str(type(self._stream_name))}")
        if self._stream_name == '' or self._stream_name == 'stream_name_here':
            raise ValueError('config-kinesis_scraper.yaml: A stream name must be set.')

        # Shard IDs
        ClientConfig.validate_shard_ids(self._shard_ids)

        # Starting position iterator Type/Timestamp
        ClientConfig.validate_iterator_types(self._starting_position)
        if self._starting_position.upper() == 'AT_TIMESTAMP':
            try:
                common.validate_datetime(self._timestamp)
            except ValueError as e:
                raise ValueError(f"config-kinesis_scraper.yaml: Invalid format for config parameter \"timestamp\". "
                                 f"Format should be YYYY-MM-DD HH:MM:SS.\nValue provided: "
                                 f"{str(type(self._timestamp))} {repr(self._timestamp)}") from e

        # Sequence Number
        self.validate_sequence_number(self._shard_ids, self._starting_position, self._sequence_number)

        # Batch Size
        try:
            common.validate_numeric(self._poll_batch_size)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"If config-kinesis_scraper.yaml: \"poll_batch_size\" must be either a numeric "
                f"string, or an integer.\nValue provided: {repr(type(self._poll_batch_size))} "
            ) from e
        if int(self._poll_batch_size) > 500:
            raise ValueError('config-kinesis_scraper.yaml: poll_batch_size cannot exceed 500')

        # Poll Delay
        try:
            common.validate_numeric(self.poll_delay)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"If config-kinesis_scraper.yaml: \"poll_delay\" must be either a numeric "
                f"string, a float, or an integer.\nValue provided: "
                f"{repr(type(self.poll_delay))} {repr(self.poll_delay)}"
            ) from e
        if float(self.poll_delay) < 0 or float(self.poll_delay) > 10:
            raise ValueError('config-kinesis_scraper.yaml: poll_delay must be between 0-10')

        # Max Empty Record Polls
        try:
            common.validate_numeric(self._max_empty_record_polls)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"If config-kinesis_scraper.yaml: \"max_empty_record_polls\" must be either a numeric "
                f"string, or an integer.\nValue provided: {repr(type(self._max_empty_record_polls))} "
            ) from e
        if int(self._max_empty_record_polls) > 1000:
            raise ValueError('config-kinesis_scraper.yaml: max_empty_record_polls cannot exceed 1000')

    def _post_init_processing(self):
        # If the starting position is not timestamp, clear the timestamp value so we don't have it set internally
        # when we are never going to use it
        if self._starting_position.upper() != 'AT_TIMESTAMP':
            self._timestamp = None
        self._starting_position = self._starting_position.upper()
        if self._poll_delay is not None:
            self._poll_delay = float(self._poll_delay)

    @staticmethod
    def validate_sequence_number(shard_ids: list, starting_position: str, sequence_number: [str, int]) -> None:
        ClientConfig.validate_iterator_types(starting_position)
        ClientConfig.validate_shard_ids(shard_ids)
        if starting_position.upper() in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']:
            # A sequence number is only unique within a shard, so we cannot specify at/after sequence
            # unless a single and only a single shard is id specified
            if len(shard_ids) != 1:
                raise ValueError(
                    f"If config-kinesis_scraper.yaml: \"starting_position\" is AT_SEQUENCE_NUMBER "
                    f"or AFTER_SEQUENCE_NUMBER, exactly 1 shard id must be specified as the sequence numbers "
                    f"are unique per shard.\nValue provided: {repr(type(shard_ids))} "
                )
            try:
                common.validate_numeric(sequence_number)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"If config-kinesis_scraper.yaml: \"starting_position\" is AT_SEQUENCE_NUMBER "
                    f"or AFTER_SEQUENCE_NUMBER, the value must be either a numeric string, or an integer. "
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
        return shard_id


class Client:
    def __init__(self, client_config: ClientConfig):
        self._client_config = client_config
        self._is_valid()

        # Setup default attributes
        self._current_shard_iterator = None
        self._current_shard_id = None
        self._current_fetched_event_count = 0

    def _is_valid(self):
        if not isinstance(self._client_config, ClientConfig):
            raise TypeError(f"client_config must be an instance of ClientConfig. Value provided: "
                            f"{repr(type(self._client_config))} {repr(self._client_config)}")
        # self._client = kinesis_client
        # self._stream_name = stream_name
        # self._shard_ids = Client.validate_shard_ids(shard_ids)
        # if self._shard_ids is None:
        #     self._shard_ids = self._get_shard_ids()
        # self._shard_id_current = None
        # self._shard_id_current = self._shard_ids[0]
        # self._shard_iterator = None
        # # After finished scraping all messages from a shard, we record it as "done/processed" in this list
        # self._shard_ids_processed = []

    def get_records(self) -> str:
        var = {
            "_boto_client": None,
            "_stream_name": "user-activities",
            "_shard_ids": [],
            "_starting_position": "AT_TIMESTAMP",
            "_timestamp": "2022-12-01 00:00:0",
            "_sequence_number": 4.963479396773268e+55,
            "_max_total_records_per_shard": 0,
            "_poll_batch_size": 100,
            "_poll_delay": 0,
            "_max_empty_record_polls": 100
        }

        i = 1
        next_iterator = self._shard_iterator('shardId-000000000005')
        count_response_no_records = 0
        found_records = []
        while next_iterator:
            log.info('get_records() loop count: ' + str(i))
            if self._client_config.poll_delay > 0:
                log.info(f"Wait delay of {self._client_config.poll_delay} seconds per poll_delay setting...")
                time.sleep(self._client_config.poll_delay)
            response = self._client_config.boto_client.get_records(
                ShardIterator=next_iterator,
                Limit=5
            )
            # log.debug(response)
            next_iterator = response['NextShardIterator']

            if len(response["Records"]) > 0:
                found_records.append(response["Records"])
                count_response_no_records = 0
                log.info(f"\n\n{len(response['Records'])} records found in get_records() response.\n")
            else:
                count_response_no_records += 1
                log.info(f'No records found in loop. Currently at {count_response_no_records - 1} empty'
                         f' sequential calls.')

            # log.debug(f'Next iterator (last 10 chars): {next_iterator[-10:]}')
            log.debug(f'Next iterator: {next_iterator}')
            self._current_shard_iterator = next_iterator
            i += 1
            if i > 5:
                pvd(found_records)
                log.info('ended loop at X calls')
                die()

        return ''

    # If we don't have an existing/current shard iterator, we grab a new one, otherwise return the current one
    def _shard_iterator(self, shard_id: str) -> str:
        if not isinstance(shard_id, str):
            raise ValueError(f"shard_id must be a string.\nType provided: {repr(type(shard_id))}")

        if self._current_shard_iterator is not None:
            return self._current_shard_iterator

        return self.get_shard_iterator(shard_id)

    def get_shard_iterator(self, shard_id: str) -> str:
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
        iterator = response['ShardIterator']
        log.debug('Returned Iterator: ' + iterator)
        return iterator

    def get_shard_ids_of_stream(self) -> list:
        log.info('Getting shard ids...')
        response = self._client_config.boto_client.describe_stream(StreamName=self._client_config.stream_name)
        log.info(f"Stream name: {response['StreamDescription']['StreamName']}")
        log.info(f"Stream ARN: {response['StreamDescription']['StreamARN']}")
        shard_ids = []
        shard_details = response['StreamDescription']['Shards']
        for node in shard_details:
            log.info(f"Found shard id: {node['ShardId']}")
            shard_ids.append(node['ShardId'])
        return shard_ids


class ShardIterator:
    def __init__(self, ):
        shard_iterator_config.is_valid()
        self._shard_iterator_config = shard_iterator_config
        self._shard_iterator = None

        pvdd(iterator)

        i = 1
        next_iterator = iterator
        while next_iterator:
            next_iterator = None
            log.debug('Loop count: ' + str(i))
            response = self._shard_iterator_config.client.get_records(
                ShardIterator=iterator,
                Limit=100
            )
            log.debug(response)
            next_iterator = response['NextShardIterator']
            i += 1
            if i > 101:
                die('ended loop at 101')

        response = self._shard_iterator_config.client.get_records(
            ShardIterator=iterator,
            Limit=10
        )
        pvdd(response)

        return response['ShardIterator']
