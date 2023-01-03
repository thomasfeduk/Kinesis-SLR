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

# Set kinesis to debug log level
kinesis_logger = logging.getLogger('includes.kinesis_client')
# kinesis_logger.setLevel(logging.DEBUG)
lambda_client_logger = logging.getLogger('includes.lambda_client')
# lambda_client_logger.setLevel(logging.DEBUG)


def main():
    records = {'Records': [
        {'SequenceNumber': '49636577181771207871059760348650819005673406360434245714',
         'ApproximateArrivalTimestamp': datetime.datetime(2023, 1, 2, 21, 2, 23, 513000,
                                                          tzinfo=tzlocal()),
         'Data': b'{"mytimestamp": "2023-01-02 21:02:21-1", "error": false, "unrecoverable": false}',
         'PartitionKey': '1'},
        {'SequenceNumber': '49636577181771207871059760348652027931493020989608951890',
         'ApproximateArrivalTimestamp': datetime.datetime(2023, 1, 2, 21, 2, 23, 515000,
                                                          tzinfo=tzlocal()),
         'Data': b'{"mytimestamp": "2023-01-02 21:02:21-2", "error": false, "unrecoverable": false}',
         'PartitionKey': '1'},
        {'SequenceNumber': '49636577181771207871059760348653236857312635618783658066',
         'ApproximateArrivalTimestamp': datetime.datetime(2023, 1, 2, 21, 2, 23, 515000,
                                                          tzinfo=tzlocal()),
         'Data': b'{"mytimestamp": "2023-01-02 21:02:21-3", "error": false, "unrecoverable": false}',
         'PartitionKey': '1'},

    ],
        'NextShardIterator': 'AAAAAAAAAAGgXiapW/Ic0EbvNhQTFaNvqqhZGpaNN2p811cF87dSdhYzIHMh3e/mmzpPYKqc3FH2IEdYoyrymDG9Vah6s4UTDxu80aR6hQtVDlxvX7KjwRiR7Tuc7z8YoBMXRDGqdzOnmZJxTcGNTd0nuLmrAvhdJFGPlhe33I/nPfrQbboFCLxoh47uyXlHFtf+kJNdfvpVQd414BBpqkUlBf4/oJDOFstLhDh85tv0pDiZpR2yfg==',
        'MillisBehindLatest': 0,
        'ResponseMetadata': {
            'RequestId': 'd79afad5-0a8e-89a2-8bb7-53664806bc1a', 'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'x-amzn-requestid': 'd79afad5-0a8e-89a2-8bb7-53664806bc1a',
                'x-amz-id-2': 'aV8ByAsJAKtuZD04SKrs4OO7A1oGkWkz/rlYthRGWvaYElTzTfWNKXnBrD2Ai+Rmy3OSCf3KY4ewzZ8Iux5jbI2Az10WOLay',
                'date': 'Tue, 03 Jan 2023 03:56:35 GMT',
                'content-type': 'application/x-amz-json-1.1',
                'content-length': '2927'
            }, 'RetryAttempts': 0
        }
    }


    kinesis_config = kinesis.ClientConfig(
        common.read_config('config-kinesis_scraper.example.yaml'),
        boto3.client('kinesis')
    )

    kinesis_client = kinesis.Client(kinesis_config)
    # output = kinesis_client._scrape_records_for_shard('shardId-000000000005')
    output = kinesis_client._process_records('shard-01', records["Records"])
    # pvdd('end')
    pvdd(output)
