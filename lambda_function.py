import boto3
import random
import base64
import json
import datetime

s3 = boto3.resource('s3')
bucket_name = 'thomasfeduk-kinesis1'


def lambda_handler(event, context):
    filename = 'Starting ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - '\
               + str(random.randrange(1, 99999))
    s3.Object(bucket_name, filename).put(Body=json.dumps(event))

    raise Exception('Oops')
