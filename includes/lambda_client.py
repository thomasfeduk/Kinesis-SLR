from typing import Union
import boto3
import botocore
import logging
import json
from includes.debug import *
import random
import datetime
import includes.common as common
import logging
import re

log = logging.getLogger()


# log.setLevel(logging.DEBUG)

class ClientConfig(common.BaseCommonClass):
    def __init__(self, passed_data: Union[dict, str], boto_client: botocore.client.BaseClient):
        self._boto_client = boto_client
        self._function_name = None
        self._batch_size = None
        self._local_dlq = None
        self._local_dlq_fullevent = None
        self._retry_attempts = None
        self._bisect_on_error = None
        self._tumbling_window_seconds = None
        self._custom_checkpoints = None

        # Have to call parent after defining attributes other they are not populated
        super().__init__(passed_data)

    @property
    def boto_client(self):
        return self._boto_client

    @property
    def function_name(self):
        return self._function_name

    @property
    def batch_size(self):
        return self._batch_size

    @property
    def local_dlq(self):
        return self._local_dlq

    @property
    def local_dlq_fullevent(self):
        return self._local_dlq_fullevent

    @property
    def retry_attempts(self):
        return self._retry_attempts

    @property
    def bisect_on_error(self):
        return self._bisect_on_error

    @property
    def tumbling_window_seconds(self):
        return self._tumbling_window_seconds

    @property
    def custom_checkpoints(self):
        return self._custom_checkpoints

    def _is_valid(self):
        if self.function_name == 'function_name_here':
            raise ValueError('config-lambda_replay.yaml: A function name must be set.')

        if self.batch_size > 10000:
            raise ValueError('config-lambda_replay.yaml: batch_size cannot exceed 10000')

        if self.retry_attempts > 5:
            raise ValueError('config-lambda_replay.yaml: retry_attempts cannot exceed 5')

        if self.tumbling_window_seconds != "N/A":
            raise ValueError('config-lambda_replay.yaml: tumbling_window_seconds is not yet supported!')

        if self.custom_checkpoints != "N/A":
            raise ValueError('config-lambda_replay.yaml: custom_checkpoints is not yet supported!')

    def _post_init_processing(self):
        pass


# config_lambda = ConfigLambda(common.read_config('config-lambda_replay.example.yaml'))
# pvdd(config_lambda)

class Client(common.BaseCommonClass):
    def __init__(self, client_config: ClientConfig):
        self._client_config = client_config

    def _invoke(self, payload):
        try:
            response = self._client_config.boto_client.invoke(FunctionName=self._client_config.function_name,
                                                              Payload=json.dumps(payload))
        except Exception as e:
            log.error(f"Error invoking Lambda function: {e}")
            return

        jout(response)
        joutd(response["Payload"].read())

        response_payload = response['Payload'].read()
        content_type = response['Payload'].content_type

        if content_type == 'application/json':
            response_payload = json.loads(response_payload)
        elif content_type == 'text/plain':
            response_payload = response_payload.decode('utf-8')
        else:
            log.warning(f"Lambda response of unknown type: {content_type}")
            response_payload = None

        if response['StatusCode'] == 200:
            log.info('Lambda invoked successfully.')
            if 'errorMessage' in response_payload:
                log.error(f"Lambda function returned an error: {response_payload['errorMessage']}")
            else:
                log.info(f"Lambda response: {response_payload}")
        else:
            log.error(f"Lambda invocation failed with status code: {response['StatusCode']}")

    def begin_processing(self):
        output = list(self._filelist_batch_iterator('scraped_events/shardId-000000000004', batch_size=5))

        pvdd(output)
        pass

    def _filelist_batch_iterator(self, directory_path: str, batch_size: int) -> list:
        files = [f for f in os.listdir(directory_path)]
        files = sorted(files,
                       key=lambda x: (int(re.search(r'^\d+', x).group()) if re.search(r'^\d+', x) else float('inf'), x))

        file_pattern = r'^\d{1,10}-\d{4}-\d{2}-\d{2}_\d{2};\d{2};\d{2}\.json$'
        for file_name in files:
            if not re.match(file_pattern, file_name):
                raise ValueError(f"Cannot begin replaying events: File name \"{file_name}\" does not match the "
                                 f"expected pattern for a Kinesis message created by the Kinesis-SLR. Please "
                                 f"corrector remove the offending file to begin replaying events.")

        for i in range(0, len(files), batch_size):
            yield files[i:i + batch_size]

    def _build_payload(self):
        ref = {
            "Records": [
                {
                    "kinesis": {
                        "kinesisSchemaVersion": "1.0",
                        "partitionKey": "testfail",
                        "sequenceNumber": "49634871856207391309887373936488347325069951854572470306",
                        "data": "eyJudW1iZXIiOiAyLCAiZXJyb3IiOiBmYWxzZX0=",
                        "approximateArrivalTimestamp": 1667723578.271
                    },
                    "eventSource": "aws:kinesis",
                    "eventVersion": "1.0",
                    "eventID": "shardId-000000000002:49634871856207391309887373936488347325069951854572470306",
                    "eventName": "aws:kinesis:record",
                    "invokeIdentityArn": "arn:aws:iam::443035303084:role/service-role/kworker-role-douez4hk",
                    "awsRegion": "us-east-1",
                    "eventSourceARN": "arn:aws:kinesis:us-east-1:443035303084:stream/user-activities"
                }
            ]
        }
