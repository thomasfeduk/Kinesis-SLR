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
    #     response_no_records=5,
    #     found_records=5,
    #     iterator="2",
    #     shard_id="3",
    # )
    # pvdd(obj)

    input = {
        "Records": [
            {
                "SequenceNumber": "49636577181771207871059760348650819005673406360434245714",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.513000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-1\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348652027931493020989608951890",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-2\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348653236857312635618783658066",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-3\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348654445783132250247958364242",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-4\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348655654708951864877133070418",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-5\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348656863634771479506307776594",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-6\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348658072560591094135482482770",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-7\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348659281486410708764657188946",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-8\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348660490412230323393831895122",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-9\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            },
            {
                "SequenceNumber": "49636577181771207871059760348661699338049938023006601298",
                "ApproximateArrivalTimestamp": "2023-01-02 21:02:23.515000-05:00",
                "Data": "b'{\"mytimestamp\": \"2023-01-02 21:02:21-10\", \"error\": false, \"unrecoverable\": false}'",
                "PartitionKey": "1"
            }
        ],
        "NextShardIterator": "AAAAAAAAAAFugp79btEuGXGYuGRBuWtEXdM+eLPAzRyeh/77iwqb+A/XeWHoR2ylCK1GEyHEMXllLwOWpKB6mKSO50kwq8gq8CqSXZpcALw41mH11U5mUpmU1kLDEXxqh1FuMMkYKg2ZdSII+Q9HQRk9j8JQRfqeigMHq4VTpo/WTgqgfrfwsH4ftHtds3UEWfQ1pl5vMAOx2wEKDOFIidg/vdox58ELqSHbfNIpmO52ct+KVC1daQ==",
        "MillisBehindLatest": 0,
        "ResponseMetadata": {
            "RequestId": "c756a849-d69a-bb8e-9b7b-01635547619c",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amzn-requestid": "c756a849-d69a-bb8e-9b7b-01635547619c",
                "x-amz-id-2": "97QiVnKfbSiSPUkgmqUrKHyvSQZuvUCa3QgKzbya33r8eTrtyc51owg7d5Q9+CE87oDwzznKAZpvM7q/lpYARKZ2yuCstAoA",
                "date": "Tue, 03 Jan 2023 03:54:18 GMT",
                "content-type": "application/x-amz-json-1.1",
                "content-length": "2927"
            },
            "RetryAttempts": 0
        }
    }
    obj = kinesis.Boto3GetRecordsResponse(input)
    pvdd(obj)

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
