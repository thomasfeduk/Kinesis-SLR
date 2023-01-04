import boto3
import logging
import json
from includes.debug import pvdd, pvd, die
import random
import datetime
import includes.common as common
import logging
from abc import ABC, abstractmethod

log = logging.getLogger()
# log.setLevel(logging.DEBUG)


class ConfigLambda(common.BaseCommonClass, ABC):
    def __init__(self, passed_data: [dict, str] = None):
        self.function_name = None
        self.batch_size = None
        self.local_dlq = None
        self.local_dlq_fullevent = None
        self.retry_attempts = None
        self.bisect_on_error = None
        self.tumbling_window_seconds = None
        self.custom_checkpoints = None

        # Have to call parent after defining attributes other they are not populated
        super().__init__(passed_data)

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
