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

# Initialize logger
logging.basicConfig()

# Set kinesis to debug log level
kinesis_logger = logging.getLogger('includes.kinesis_client')
# kinesis_logger.setLevel(logging.DEBUG)
lambda_client_logger = logging.getLogger('includes.lambda_client')
# lambda_client_logger.setLevel(logging.DEBUG)


def main():

    kinesis_config = kinesis.ClientConfig(
        common.read_config('config-kinesis_scraper.example.yaml'),
        boto3.client('kinesis')
    )

    kinesis_client = kinesis.Client(kinesis_config)
    output = kinesis_client.get_records
    # pvdd('end')
    pvdd(output)
