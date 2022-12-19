import boto3
import logging
import json
from debug import pvdd, pvd, die
import random
import datetime
import common
import logging

log = logging.getLogger()


# log.setLevel(logging.DEBUG)

class ShardIteratorConfig:
    def __init__(self, *,
                 kinesis_client: object,
                 stream_name: str,
                 shard_id: str,
                 iterator_type: str,
                 timestamp: str = None
                 ):
        self.client = kinesis_client
        self.stream_name = stream_name
        self.shard_id = shard_id
        self.iterator_type = iterator_type
        self.timestamp = timestamp
        self.is_valid()

    def is_valid(self):
        validate_shard_id(self.shard_id)
        validate_iterator_types(self.iterator_type)
        if self.timestamp is not None:
            validate_datetime(self.timestamp)


def validate_shard_ids(shard_ids: list = None):
    # If we are dealing with a blank list of shard ids (None)
    # we want to proceed as that indicates we will be processing all shards
    if shard_ids is None:
        return None

    if not isinstance(shard_ids, list):
        raise TypeError('shard_ids must be of type list if specified. Type provided: '
                        + str(type(shard_ids)))

    for shard_id in shard_ids:
        validate_shard_id(shard_id)
    return shard_ids


def validate_shard_id(shard_id: str = None):
    if not isinstance(shard_id, str):
        raise TypeError('Each shard_id must be a string. Value provided: '
                        + repr(type(shard_id))
                        + ' '
                        + repr(shard_id))
    return shard_id


def validate_iterator_types(iterator_type):
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


def validate_datetime(timestamp):
    try:
        datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD HH:MM:SS")
    return timestamp


class Client:
    def __init__(self, kinesis_client: object, stream_name: str, shard_ids: list = None):
        self._client = kinesis_client
        self._stream_name = stream_name
        self._shard_ids = validate_shard_ids(shard_ids)
        if self._shard_ids is None:
            self._shard_ids = self._get_shard_ids()
        self._shard_id_current = None
        self._shard_id_current = self._shard_ids[0]
        self._shard_iterator = None
        # After finished scraping all messages from a shard, we record it as "done/processed" in this list
        self._shard_ids_processed = []

    def get_records(self, iterator_type: str, limit: int = 100, *, timestamp: str = None) -> str:
        if self._shard_iterator is None:
            self._shard_iterator = ShardIterator(
                ShardIteratorConfig(
                    kinesis_client=self._client,
                    stream_name=self._stream_name,
                    shard_id=self._shard_id_current,
                    iterator_type=iterator_type,
                    timestamp=timestamp
                )
            )
        return self._shard_iterator.get_iterator()

    def _get_shard_ids(self) -> list:
        response = self._client.describe_stream(StreamName=self._stream_name)
        pvdd(response)
        shard_ids = []
        shard_details = response['StreamDescription']['Shards']
        for node in shard_details:
            shard_ids.append(node['ShardId'])
        return shard_ids


class ShardIterator:
    def __init__(self, shard_iterator_config: ShardIteratorConfig):
        shard_iterator_config.is_valid()
        self._shard_iterator_config = shard_iterator_config
        self._shard_iterator = None

    def get_iterator(self) -> str:
        # If we have a timestamp specified, we call client.get_shard_iterator with the timestamp,
        # otherwise call it without that argument
        log.debug('Getting iterator for shard id: ' + self._shard_iterator_config.shard_id)
        if self._shard_iterator_config.timestamp is None:
            response = self._shard_iterator_config.client.get_shard_iterator(
                StreamName=self._shard_iterator_config.stream_name,
                ShardId=self._shard_iterator_config.shard_id,
                ShardIteratorType=self._shard_iterator_config.iterator_type,
            )
        else:
            response = self._shard_iterator_config.client.get_shard_iterator(
                StreamName=self._shard_iterator_config.stream_name,
                ShardId=self._shard_iterator_config.shard_id,
                ShardIteratorType=self._shard_iterator_config.iterator_type,
                Timestamp=self._shard_iterator_config.timestamp
            )
        iterator = response['ShardIterator']
        log.debug('Returned Iterator: ' + iterator)

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


class ConfigScraper(common.ConfigSLR):
    def __init__(self, passed_data: [dict, str] = None):
        self.stream_name = None
        self.shardIds = []
        self.starting_position = None
        self.timestamp = None
        self.sequence_number = None
        self.max_total_records_per_shard = None
        self.poll_batch_size = None
        self.poll_delay = None
        self.max_empty_record_polls = None

        # Have to call parent after defining attributes other they are not populated
        super().__init__(passed_data)

    def is_valid(self):
        validate_shard_ids(self.shardIds)
        validate_iterator_types(self.starting_position)
        if self.starting_position.upper() == 'TIMESTAMP':
            validate_datetime(self.timestamp)
        if self.starting_position.upper() in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']:
            if isinstance(self.sequence_number, str) is False and isinstance(self.sequence_number, int) is False:
                raise TypeError(f"If config-kinesis-scraper property starting_position is AT_SEQUENCE_NUMBER or "
                                f"AFTER_SEQUENCE_NUMBER, the value must be either a numeric string, or an integer. "
                                f"Value provided: {repr(type(self.starting_position))} {repr(self.starting_position)}")


config_kinesis = ConfigScraper(common.read_config('config-kinesis_scraper.example.yaml'))
pvdd(config_kinesis)