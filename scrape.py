import boto3
import logging
import json
from debug import pvdd
from debug import die
import random
import kinesis


# def create_fanout():


def process_events(Events):
    # ---------------------------
    # Your processing goes here
    # ---------------------------

    for event in Events:
        pvdd(event)
        f = open("demofile2.txt", "w")
        # f.write("Now the file has more content!")
        json.dump(event, f, default=str)
        f.close()
        pvdd(event)


def get_shard_iterator(*, stream: str, shard_id: str = None):
    pass


def main():
    stream_name = 'user-activities'

    client = kinesis.Client(boto3.client('kinesis'), stream_name)
    records = client.get_records('TRIM_HORIZON', 100)
    pvdd(records)

    # ------------------
    # Get the shard ID.
    # ------------------
    kinesis_client = boto3.client('kinesis')
    response = kinesis_client.describe_stream(StreamName=stream_name)

    shard_id = response['StreamDescription']['Shards'][2]['ShardId']
    pvdd(response)

    # ---------------------------------------------------------------------------------------------
    # Get the shard iterator.
    # ShardIteratorType=AT_SEQUENCE_NUMBER|AFTER_SEQUENCE_NUMBER|TRIM_HORIZON|LATEST|AT_TIMESTAMP
    # ---------------------------------------------------------------------------------------------
    response = kinesis_client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON',
        # Timestamp=1662724626
    )

    shard_iterator = response['ShardIterator']
    # print("Shard iterator: " + shard_iterator)

    # -----------------------------------------------------------------
    # Get the records.
    # Get max_records from the shard, or run continuously if you wish.
    # -----------------------------------------------------------------
    max_records = 1000
    record_count = 0
    records = []  # Stores all read records

    # Iterate through 1000 records at a time and assemble them into records
    while record_count < max_records:
        print('Looping')
        response = kinesis_client.get_records(
            ShardIterator=shard_iterator,
            Limit=10
        )
        pvdd(response)
        shard_iterator = response['NextShardIterator']
        records.append(response['Records'])
        record_count += len(records)

    # After obtaining all records, process them
    # process_events(records)

    pvdd(len(records))


if __name__ == "__main__":
    main()
