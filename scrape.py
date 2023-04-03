import jsonpickle

from includes.debug import *
from datetime import datetime
import shutil
import includes.stdout_unbuffered
import boto3
import logging
import json
import random
import includes.kinesis_client as kinesis
import includes.lambda_client as lambda_client
import logging
import includes.common as common
import re
import pickle
import datetime
from dateutil.tz import tzlocal
from tests.test_kinesis_client import generate_Boto3GetRecordsResponse
import uuid
from var_dump import var_dump

# Initialize logger
logging.basicConfig()

import os


def main_lambda():
    # lambda_client = boto3.client('lambda', "us-east-1")
    # input_payload = {'key1': 'value1', 'key2': 'value2'}
    # response = lambda_client.invoke(
    #     FunctionName='kworker',
    #     Payload=json.dumps(input_payload)
    # )
    # print(json.dumps(response, indent=4, default=str))
    # print(json.dumps(json.loads(response['Payload'].read().decode('utf-8')), indent=4))
    # die('sdsadad')

    config_yaml = common.read_config('config-lambda_replay.yaml')
    lambda_config = lambda_client.ClientConfig(config_yaml, boto3.client('lambda', "us-east-1"))
    client = lambda_client.Client(lambda_config)
    client.begin_processing()

    die('scrape.py;: main_lambda()')

    # client = boto3.client("sts")
    # var = client.get_caller_identity()
    # pvdd(var)





def main_kinesis():
    # pvdd([
    #     generate_Boto3GetRecordsResponse(3, data_prefix="boto3resp", iterator_prefix="iter1"),
    #     generate_Boto3GetRecordsResponse(3, data_prefix="boto3resp", iterator_prefix="iter2"),
    #     generate_Boto3GetRecordsResponse(3, data_prefix="boto3resp", iterator_prefix="iter3"),
    #     generate_Boto3GetRecordsResponse(3, data_prefix="boto3resp", iterator_prefix="iter4"),
    # ])
    # serialized get_records().Records response:
    record_serialized = b'\x80\x04\x95\xda\x01\x00\x00\x00\x00\x00\x00}\x94(' \
                        b'\x8c\x0eSequenceNumber\x94\x8c849636577182105719049037919318577501443478516337397989442\x94' \
                        b'\x8c\x1bApproximateArrivalTimestamp\x94\x8c\x08datetime\x94\x8c\x08datetime\x94\x93\x94C\n' \
                        b'\x07\xe7\x01\x0e\x14\x17\x14\x07GH\x94\x8c\x0edateutil.tz.tz\x94\x8c\x07tzlocal\x94\x93\x94' \
                        b')\x81\x94}\x94(\x8c\x0b_std_offset\x94h\x04\x8c\ttimedelta\x94\x93\x94J\xff\xff\xff\xffJ0' \
                        b'\x0b\x01\x00K\x00\x87\x94R\x94\x8c\x0b_dst_offset\x94h\x0fJ\xff\xff\xff\xffJ@\x19\x01\x00K' \
                        b'\x00\x87\x94R\x94\x8c\n_dst_saved\x94h\x0fK\x00M\x10\x0eK\x00\x87\x94R\x94\x8c\x07_hasdst' \
                        b'\x94\x88\x8c\x08_tznames\x94\x8c\x15Eastern Standard Time\x94\x8c\x15Eastern Daylight ' \
                        b'Time\x94\x86\x94ub\x86\x94R\x94\x8c\x04Data\x94CP{"mytimestamp": "2023-01-14 20:23:19-1", ' \
                        b'"error": false, "unrecoverable": false}\x94\x8c\x0cPartitionKey\x94\x8c\x011\x94u. '

    # iteration_input = kinesis.GetRecordsIterationResponse(
    #     total_found_records=10,
    #     found_records=5,
    #     response_no_records=2,
    #     loop_count="15",
    #     next_shard_iterator="abc",
    #     shard_id="shard-123",
    #     break_iteration=True
    # )
    #
    # pvdd('here123')

    # results = client.get_shard_iterator(
    #     StreamName='user-activities',
    #     ShardId='shardId-000000000004',
    #     ShardIteratorType='AT_TIMESTAMP',
    #     Timestamp='dsadsdsd'
    # )
    # pvdd(results)

    # obj = kinesis.GetRecordsIteratorInput(
    #     total_found_records=21,
    #     response_no_records=5,
    #     loop_count=0,
    #     shard_iterator="2",
    #     shard_id="3",
    # )
    # obj.total_found_records = 123
    # pvdd(obj.total_found_records)

    # obj = kinesis.GetRecordsIteratorResponse(
    #     total_found_records=10,
    #     found_records=5,
    #     response_no_records=5,
    #     loop_count=0,
    #     next_shard_iterator="2",
    #     shard_id="3",
    #     break_iteration=False
    # )
    # del obj._proptypes
    # del obj._require_numeric_pos
    # pvdd(obj.total_found_records)

    # Delete any existing local files
    # try:
    #     dir_path = 'scraped_events/shardId-000000000005'
    #     shutil.rmtree(dir_path)
    # except FileNotFoundError as ex:
    #     print(f'No old scraped events to delete...: {repr(ex)}')

    config_yaml = common.read_config('config-kinesis_scraper.yaml')
    kinesis_config = kinesis.ClientConfig(config_yaml, boto3.client('kinesis', config_yaml['region_name']))

    output = None
    kinesis_client = kinesis.Client(kinesis_config)
    output = kinesis_client.begin_scraping()
    # kinesis_client._scrape_records_for_shard('shardId-000000000005')
    # output = kinesis_client._process_records('shard-01', records["Records"])
    # pvdd('end')
    pvd('scrape.py end')
    pvdd(output)
