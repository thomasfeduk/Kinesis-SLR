import shutil

import includes.stdout_unbuffered
import boto3
import logging
import json
from includes.debug import pvdd, pvd, die
import random
import includes.kinesis_client as kinesis
import includes.lambda_client as lambda_client
import logging
import includes.common as common
import datetime
from dateutil.tz import tzlocal

# Initialize logger
logging.basicConfig()


def main():
    # obj = kinesis.GetRecordsIteratorInput(
    #     found_records=5,
    #     response_no_records=5,
    #     loop_count=0,
    #     shard_iterator="2",
    #     shard_id="3",
    # )
    # pvdd(obj)

    obj = kinesis.GetRecordsIteratorOutput(
        found_records=5,
        response_no_records=5,
        loop_count=0,
        shard_iterator="2",
        shard_id="3",
        break_iteration=False
    )
    pvdd(obj.break_iteration)

    # Delete any existing local files
    try:
        dir_path = 'scraped_events/shardId-000000000005'
        shutil.rmtree(dir_path)
    except Exception as ex:
        print(f'Could not delete old scraped events: {repr(ex)}')

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
