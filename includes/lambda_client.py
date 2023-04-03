import shutil
from typing import Union, Any
import boto3
import botocore
import logging
import json

import includes.kinesis_client as kinesis_client
from includes.debug import *
import random
import datetime
import includes.common as common
import logging
import re
from typing import List
import includes.exceptions as exceptions

log = logging.getLogger(__name__)
log.setLevel("DEBUG")


class Files(common.Collection):
    def __init__(self, *, shard_id: str = None, file_list: list = None):
        if (shard_id is None and file_list is None) or (shard_id is not None and file_list is not None):
            raise exceptions.InvalidArgumentException("Exactly one of `shard_id` or `file_list` must be provided.")

        items = []
        if shard_id is not None:
            items = self._init_from_shard_id(shard_id)

        if file_list is not None:
            items = self._init_from_file_list(file_list)

        # Have to call parent to populate self._items
        super().__init__(items)

    def _init_from_shard_id(self, shard_id):
        common.require_type(shard_id, str, exceptions.InvalidArgumentException)
        self._shard_id = shard_id
        self._dir_path = f'scraped_events/{self._shard_id}'
        self._is_valid()
        files_unsorted: List[str] = [f for f in os.listdir(self._dir_path)]
        items: List[str] = sorted(files_unsorted,
                                  key=lambda x: (
                                      int(re.search(r'^\d+', x).group()) if re.search(r'^\d+', x) else float(
                                          'inf'), x))
        return items

    def _init_from_file_list(self, items: list):
        common.require_type(items, list)
        return items

    def _is_valid(self) -> None:
        try:
            kinesis_client.ClientConfig.validate_shard_id(self._shard_id)
        except ValueError as ex:
            raise exceptions.InvalidArgumentException(ex) from ex
        if not os.path.exists(self._dir_path):
            raise exceptions.InvalidArgumentException(f"Scrapped shard directory {self._shard_id} does not exist.")

    @staticmethod
    def validate_file_name(file_name: str):
        common.require_type(file_name, str)
        file_pattern = r'^\d{1,10}-\d{4}-\d{2}-\d{2}_\d{2};\d{2};\d{2}\.json$'
        if not re.match(file_pattern, file_name):
            raise ValueError(f"Invalid file name format. {Files.expected_pattern_error(file_name)}")

    @staticmethod
    def expected_pattern_error(file_name: Any):
        return f"Expected pattern: {str} 'X-YYYY-MM-DD_HH;MM;SS' Received: {common.type_repr(file_name)}"


class FileListBatchIterator(common.Collection):
    def __init__(self, files_obj: Files, shard_id: str, batch_size: int) -> None:
        common.require_type(files_obj, Files)
        common.require_type(shard_id, str)
        common.require_type(batch_size, int)

        self._shard_id: str = shard_id
        self._batch_size: int = batch_size

        # Have to call parent to populate self._items
        super().__init__(list(files_obj))

        for file_name in self._items:
            try:
                Files.validate_file_name(file_name)
            except ValueError as ex:
                raise exceptions.FileProcessingError(
                    f"Cannot begin replaying events: File name '{file_name}' does not match the expected pattern "
                    f"for a Kinesis message created by the Kinesis-SLR. Please correct or remove the offending file to "
                    f"begin replaying events. {Files.expected_pattern_error(file_name)}") from ex

    @property
    def shard_id(self) -> str:
        return self._shard_id

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def items(self) -> Files:
        return Files(file_list=self._items)

    def __next__(self) -> Files:
        if self._current_index >= len(self._items):
            raise StopIteration

        batch: List[str] = self._items[self._current_index:self._current_index + self._batch_size]
        self._current_index += self._batch_size
        return Files(file_list=batch)


class ClientConfig(common.BaseCommonClass):
    def __init__(self, passed_data: Union[dict, str], boto_client: botocore.client.BaseClient):
        self._boto_client = boto_client
        self._debug_level = None
        self._region_name = None
        self._function_name = None
        self._stream_name = None
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
    def debug_level(self):
        return self._debug_level

    @property
    def region_name(self):
        return self._region_name

    @property
    def function_name(self):
        return self._function_name

    @property
    def stream_name(self):
        return self._stream_name

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
        common.require_instance(self.boto_client, botocore.client.BaseClient)
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
        super()._post_init_processing()
        # Setup logging
        log.setLevel(self._debug_level)


class Client:
    def __init__(self, client_config: ClientConfig):
        common.require_instance(client_config, ClientConfig, exceptions.InvalidArgumentException)
        self._client_config = client_config
        self._account_id = "12345"

    def _invoke(self, payload):
        response = self._client_config.boto_client.invoke(FunctionName=self._client_config.function_name,
                                                          Payload=json.dumps(payload))

        error_occurred = False
        status = "Success"
        if "FunctionError" in response.keys():
            log.error(f"Invocation failed: {response}")
            error_occurred = True
            status = "Failed"

        log.info(f'Invocation request ID: {response["ResponseMetadata"]["RequestId"]} - {status}')
        log.debug("Invocation Status: " + json.dumps(response, indent=4, default=str))
        log.debug("Invocation Response: " + json.dumps(json.loads(response["Payload"].read()), indent=4))

        if error_occurred:
            raise exceptions.AwsErrorLambdaInvocationFailed(response["ResponseMetadata"]["RequestId"])

    def begin_processing(self):
        dir_path = "scraped_events"
        file_list = os.listdir(dir_path)
        shards_ids = []
        for shard_id in file_list:
            filepath = os.path.join(dir_path, shard_id)
            if os.path.isdir(filepath):
                shards_ids.append(shard_id)
                try:
                    kinesis_client.ClientConfig.validate_shard_id(shard_id)
                except [TypeError, ValueError] as ex:
                    raise exceptions.FileProcessingError(
                        f"Cannot begin replaying events: One of the scrapped shard_id directories does not match the "
                        f"expected  pattern for a Kinesis message created by the Kinesis-SLR. Please correct or remove "
                        f"the offending file to begin replaying events {ex}") from ex

        for shard_id in shards_ids:
            self._process_shard_dir(shard_id)

    def _process_shard_dir(self, shard_id: str):
        common.require_type(shard_id, str)

        file_batch_obj = FileListBatchIterator(
            Files(shard_id=shard_id), shard_id, batch_size=self._client_config.batch_size)

        if len(list(file_batch_obj)) < 1:
            log.info(f"Shard directory {shard_id} contains no records. Skipping.")
            return

        self._precheck_files_batch_iterator(file_batch_obj)

        for files_batch in file_batch_obj:
            self._process_batch(shard_id, files_batch)

    def _precheck_files_batch_iterator(self, file_batch_iterator: FileListBatchIterator):
        """
        Reads every single message in the scrapped directory about to be processed to ensure the valid format
        of every event file. We don't want to begin processing then encounter a bad message on disk halfway through.

        Args: file_iterator (FileListBatchIterator): Files from the shard directory about to be replayed

        Returns: None.
        """
        common.require_type(file_batch_iterator, FileListBatchIterator, exceptions.InvalidArgumentException)
        try:
            kinesis_client.ClientConfig.validate_shard_id(file_batch_iterator.shard_id)
        except [TypeError, ValueError] as ex:
            raise exceptions.InvalidArgumentException(ex) from ex

        log.info(f"Verifying integrity of all files for shard {file_batch_iterator.shard_id} before replay begins...")
        i = 0
        for file in list(file_batch_iterator.items):
            i += 1
            with open(f"scraped_events/{file_batch_iterator.shard_id}/{file}", 'r') as f:
                contents = f.read()
                try:
                    kinesis_client.Record(contents)
                except Exception as ex:
                    raise exceptions.FileProcessingError(
                        f"Cannot begin replaying events: Scrapped file '{file_batch_iterator.shard_id}/{file}' is not "
                        f"in the expected format. Please correct or remove the offending file to begin replaying "
                        f"events. Detailed error: {ex}") from ex

        log.info(f"Scan complete: All {i} files for shard {file_batch_iterator.shard_id}"
                 f" are in the expected format.")

    def _process_batch(self, shard_id: str, file_list: Files):
        common.require_type(shard_id, str, exceptions.InvalidArgumentException)
        common.require_type(file_list, Files, exceptions.InvalidArgumentException)

        payload = self._build_payload(shard_id, file_list)
        batch_range_label = file_list[0]
        if len(list(file_list)) > 1:
            batch_range_label = f"{file_list[0]}..{file_list[-1]}"
        log.info(f"Processing batch: '{shard_id}/[{batch_range_label}]'")

        try:
            self._invoke(payload)
        except exceptions.AwsErrorLambdaInvocationFailed as ex:
            log.info(f"Detected failed invocation for batch. Writing to local dlq...")
            self._dlq(shard_id, file_list)

    def _dlq(self, shard_id: str, file_list: Files):
        try:
            kinesis_client.ClientConfig.validate_shard_id(shard_id)
        except [TypeError, ValueError] as ex:
            raise exceptions.InvalidArgumentException(ex) from ex
        common.require_type(file_list, Files, exceptions.InvalidArgumentException)

        dir_path = f'dlq/{shard_id}'
        log.debug(f'mkdirs path: {dir_path}')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        log.info(f"Writing the following {len(list(file_list))} messages to dlq: {', '.join(list(file_list))}")

        # Copy the files to the destination directory
        for file in file_list:
            shutil.copy(f"scraped_events/{shard_id}/{file}", dir_path)
        log.info(f"{len(list(file_list))} files written to dlq successfully.")

    def _build_payload(self, shard_id: str, file_list: Files) -> dict:
        common.require_type(shard_id, str, exceptions.InvalidArgumentException)
        common.require_type(file_list, Files, exceptions.InvalidArgumentException)
        final_payload = {"Records": []}

        for file in list(file_list):
            final_payload["Records"].append(self._build_payload_inner(shard_id, file))

        return final_payload

    def _build_payload_inner(self, shard_id: str, file: str) -> dict:
        try:
            kinesis_client.ClientConfig.validate_shard_id(shard_id)
        except [TypeError, ValueError] as ex:
            raise exceptions.InvalidArgumentException(ex) from ex
        Files.validate_file_name(file)

        with open(f"scraped_events/{shard_id}/{file}", 'r') as f:
            contents = f.read()
            record = kinesis_client.Record(contents)

        inner_payload = {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": record.PartitionKey,
                "sequenceNumber": record.SequenceNumber,
                "data": record.Data,
                "approximateArrivalTimestamp":
                    datetime.datetime.fromisoformat(record.ApproximateArrivalTimestamp).timestamp()
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": f"{shard_id}:{record.SequenceNumber}",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": f"local::kinesis-slr",
            "awsRegion": self._client_config.region_name,
            "eventSourceARN": f"arn:aws:kinesis:us-east-1:{self._account_id}:stream/{self._client_config.stream_name}"
        }

        return inner_payload
