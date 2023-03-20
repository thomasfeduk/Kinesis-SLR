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
import pickle
import datetime
from dateutil.tz import tzlocal
from tests.test_kinesis_client import generate_Boto3GetRecordsResponse
import uuid
from var_dump import var_dump

# Initialize logger
logging.basicConfig()

import os
import psutil


def format_size(size_bytes):
    # Define suffixes and their corresponding units
    suffixes = ["B", "KB", "MB", "GB"]
    base = 1024

    # Determine the appropriate suffix and scale the size accordingly
    for i, suffix in enumerate(suffixes):
        if size_bytes < base ** (i + 1):
            size = size_bytes / base ** i
            return f"{size:.2f} {suffix}"

    # If the size is very large, use the largest suffix
    size = size_bytes / base ** (len(suffixes) - 1)
    return f"{size:.2f} {suffixes[-1]}"


class LambdaPayload:
    def __init__(self):
        self.records = {}

    def addRecord(self, record, index: str):
        # validate and transform the record
        # (in this example, we just add it to the list of records)
        self.records[index] = record

    def submit(self):
        # do something with the processed records
        # print(f"Processed {len(self.records)} records")
        pvd(self.records)


class FileProcessor:
    def __init__(self, directory_path, batch_size=10):
        self.directory_path = directory_path
        self.batch_size = batch_size
        self.file_paths = [os.path.join(self.directory_path, f) for f in os.listdir(self.directory_path)]
        pvdd(self.file_paths)

    def read_files_in_batches(self):
        lambda_payload = LambdaPayload()
        for i in range(0, len(self.file_paths), self.batch_size):

            # get the current batch of file paths
            batch = self.file_paths[i:i + self.batch_size]
            pvdd(batch)

            # process the batch of files
            for file_path in batch:
                with open(file_path, 'r') as f:
                    # read the file contents
                    file_contents = f.read()

                    # add the file contents to the LambdaPayload object
                    lambda_payload.addRecord(file_contents, file_path)

            # submit the processed records after processing each batch
            lambda_payload.submit()


def main_lambda():

    var = (f for f in os.scandir('scraped_events/shardId-000000000004'))
    pvdd(list(var)[0].name)
    die()

    # my_list = [f"scraped_events/shardId-000000000004/{i}-2023-03-19_23;44;23.json" for i in range(0, 100_000)]
    my_list = [i for i in range(0, 100_000)]
    list_size = sys.getsizeof(my_list)
    print(f"The size of my_list is {format_size(list_size)}.")

    memory_usage = psutil.Process().memory_info().rss
    print(f"Python is currently using {format_size(memory_usage)} of memory.")

    pvdd(f"List count: {len(my_list)}")

    processor = FileProcessor('scraped_events/shardId-000000000004', batch_size=20)
    processor.read_files_in_batches()

    die('scrape.py lambda')

    # boto3response_raw = b'\x80\x04\x95\x1f\x07\x00\x00\x00\x00\x00\x00\x8c\x17includes.kinesis_client\x94\x8c\x17Boto3GetRecordsResponse\x94\x93\x94)\x81\x94}\x94(\x8c\x08_Records\x94h\x00\x8c\x11RecordsCollection\x94\x93\x94)\x81\x94}\x94(\x8c\x05_last\x94K\x00\x8c\x06_items\x94]\x94(h\x00\x8c\x06Record\x94\x93\x94)\x81\x94}\x94(\x8c\x0f_SequenceNumber\x94\x8c849637936497446807206520786095242010868917136376916344898\x94\x8c\x1c_ApproximateArrivalTimestamp\x94\x8c\x08datetime\x94\x8c\x08datetime\x94\x93\x94C\n\x07\xe7\x03\x13\x12\x1e\x06\x022\x80\x94\x8c\x0edateutil.tz.tz\x94\x8c\x07tzlocal\x94\x93\x94)\x81\x94}\x94(\x8c\x0b_std_offset\x94h\x14\x8c\ttimedelta\x94\x93\x94J\xff\xff\xff\xffJ0\x0b\x01\x00K\x00\x87\x94R\x94\x8c\x0b_dst_offset\x94h\x1fJ\xff\xff\xff\xffJ@\x19\x01\x00K\x00\x87\x94R\x94\x8c\n_dst_saved\x94h\x1fK\x00M\x10\x0eK\x00\x87\x94R\x94\x8c\x07_hasdst\x94\x88\x8c\x08_tznames\x94\x8c\x15Eastern Standard Time\x94\x8c\x15Eastern Daylight Time\x94\x86\x94ub\x86\x94R\x94\x8c\x05_Data\x94CP{"mytimestamp": "2023-03-19 18:30:06-1", "error": false, "unrecoverable": false}\x94\x8c\r_PartitionKey\x94\x8c\x011\x94\x8c\n_proprules\x94\x8c\x0fincludes.common\x94\x8c\tPropRules\x94\x93\x94)\x81\x94}\x94(\x8c\x06_types\x94}\x94(\x8c\x0eSequenceNumber\x94]\x94\x8c\x08builtins\x94\x8c\x03str\x94\x93\x94a\x8c\x0cPartitionKey\x94]\x94h?a\x8c\x1bApproximateArrivalTimestamp\x94]\x94(h\x16h?eu\x8c\x08_numeric\x94]\x94\x8c\x11_numeric_positive\x94]\x94ububh\x0e)\x81\x94}\x94(h\x11\x8c849637936497446807206520786095243219794736751006091051074\x94h\x13h\x16C\n\x07\xe7\x03\x13\x12\x1e\x06\x02>8\x94h\x1a)\x81\x94}\x94(h\x1dh\x1fJ\xff\xff\xff\xffJ0\x0b\x01\x00K\x00\x87\x94R\x94h"h\x1fJ\xff\xff\xff\xffJ@\x19\x01\x00K\x00\x87\x94R\x94h%h\x1fK\x00M\x10\x0eK\x00\x87\x94R\x94h(\x88h)h,ub\x86\x94R\x94h/CP{"mytimestamp": "2023-03-19 18:30:06-2", "error": false, "unrecoverable": false}\x94h1h2h3h6)\x81\x94}\x94(h9}\x94(h;]\x94h?ah@]\x94h?ahB]\x94(h\x16h?euhD]\x94hF]\x94ububh\x0e)\x81\x94}\x94(h\x11\x8c849637936497446807206520786095244428720556365635265757250\x94h\x13h\x16C\n\x07\xe7\x03\x13\x12\x1e\x06\x02>8\x94h\x1a)\x81\x94}\x94(h\x1dh\x1fJ\xff\xff\xff\xffJ0\x0b\x01\x00K\x00\x87\x94R\x94h"h\x1fJ\xff\xff\xff\xffJ@\x19\x01\x00K\x00\x87\x94R\x94h%h\x1fK\x00M\x10\x0eK\x00\x87\x94R\x94h(\x88h)h,ub\x86\x94R\x94h/CP{"mytimestamp": "2023-03-19 18:30:06-3", "error": false, "unrecoverable": false}\x94h1h2h3h6)\x81\x94}\x94(h9}\x94(h;]\x94h?ah@]\x94h?ahB]\x94(h\x16h?euhD]\x94hF]\x94ububeub\x8c\x12_NextShardIterator\x94\x8c\xf8AAAAAAAAAAH6zniUzkkCQPr/VW61uveYvRqFOPbmmCGUjSCpWzH1SByRMTeJIF6k/E4VK+mWgrDb3FYFiS98Qnf5RsJHa3Y8zoSpskiNt7L3A5Si6xFfdP4A5EWqTkqzFUuK/q0CR1whQiQUYApWu8TRGZx7XXzvOMzzaQ5X7PUWuwO96sVX6+6isfC8pgprRBuBMoCJYEqSKxExIeAGpF3F9rAUrFC/VSoPTYS3fQIYBvzguDDXhQ==\x94\x8c\x13_MillisBehindLatest\x94K\x00h3h6)\x81\x94}\x94(h9}\x94(\x8c\x07Records\x94]\x94h\x07a\x8c\x11NextShardIterator\x94]\x94h?a\x8c\x12MillisBehindLatest\x94]\x94h=\x8c\x03int\x94\x93\x94auhD]\x94hF]\x94ubub.'
    #
    # pvdd(boto3response_raw)
    #
    # boto3response_obj = pickle.loads(boto3response_raw)
    # records_collection = boto3response_obj.Records
    # print(records_collection.toJson(indent=4))
    # die('end')

    # lambda_invoker = lambda_client.Client('kworker')
    # payload = {'key1': 'value1', 'key2': 'value2'}
    # lambda_invoker.invoke(payload)


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
    try:
        dir_path = 'scraped_events/shardId-000000000005'
        shutil.rmtree(dir_path)
    except FileNotFoundError as ex:
        print(f'No old scraped events to delete...: {repr(ex)}')

    kinesis_config = kinesis.ClientConfig(
        common.read_config('config-kinesis_scraper.example.yaml'),
        boto3.client('kinesis')
    )

    output = None
    kinesis_client = kinesis.Client(kinesis_config)
    output = kinesis_client.begin_scraping()
    # kinesis_client._scrape_records_for_shard('shardId-000000000005')
    # output = kinesis_client._process_records('shard-01', records["Records"])
    # pvdd('end')
    pvd('scrape.py end')
    pvdd(output)
