import boto3
from var_dump import var_dump
import json
import boto3
import random

s3 = boto3.resource('s3')
bucket_name = 'thomasfeduk-kinesis1'

value = random.randint(1, 999999)

filename = "test"
# Creating an empty file called "_DONE" and putting it in the S3 bucket
s3.Object(bucket_name, filename + '.txt').put(Body="test123")


response = client.describe_stream(StreamName="user-activities")
# var_dump(describe_stream_result)


my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

# We use ShardIteratorType of LATEST which means that we start to look
# at the end of the stream for new incoming data. Note that this means
# you should be running the this lambda before running any write lambdas
#
shard_iterator = client.get_shard_iterator(StreamName="user-activities",
                                                   ShardId=my_shard_id,
                                                   ShardIteratorType='LATEST')
#
# # get your shard number and set up iterator
my_shard_iterator = shard_iterator['ShardIterator']

# var_dump(my_shard_id)
var_dump(shard_iterator)
# var_dump(my_shard_iterator)


record_response = client.get_records
while 'NextShardIterator' in record_response:
    # read up to 100 records at a time from the shard number
    record_response = client.get_records
    var_dump(record_response)
    exit(0)
    # # Print only if we have something
    # if(record_response['Records']):
    #     var_dump (record_response)