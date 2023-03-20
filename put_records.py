from var_dump import var_dump
import boto3
import json
import random
import datetime
from includes.debug import *
stream_name = 'user-activities'

# Boot
kinesis = boto3.client('kinesis')

# Config
events_total = 50  # Max 500
errors = []
errors_unrecoverable = []
randomize_shards = False


def publish_to_stream():
    events = generate_events()

    response = kinesis.put_records(
        Records=events,
        StreamName=stream_name
    )
    var_dump(response)


def generate_events():
    events = []
    i = 0
    while i < events_total:
        error = False
        unrecoverable = False
        if i + 1 in errors:
            error = True
        if i + 1 in errors_unrecoverable:
            unrecoverable = True

        partition_key = "1"
        if randomize_shards:
            partition_key = str(random.randrange(0, 999999))
        events.append({
            'Data': json.dumps({
                "mytimestamp": f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}-{i + 1}",
                "error": error,
                "unrecoverable": unrecoverable,
            }),
            'ExplicitHashKey': '0',
            # 'ExplicitHashKey': '170141183460469231731687303715884105728',
            'PartitionKey': partition_key
        })
        i += 1
    return events


if __name__ == "__main__":
    publish_to_stream()
