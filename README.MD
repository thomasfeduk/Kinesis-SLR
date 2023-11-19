# Kinesis-SLR

Kinesis-SLR (Kinesis Scraper and Lambda Replay) is a Python program that facilitates reading and replaying messages from an Amazon Kinesis stream. It is designed especially for disaster recovery situations where services may have missed consuming messages or message processing failed, and replays need to occur.

With Kinesis-SLR, you can connect to any point in the stream and scrape messages with surgical precision across either single or all shards as needed, read them to disk locally, and then replay them back to a Lambda function in configurable batches, mimicking the AWS Kinesis infrastructure. This allows you to recover and reprocess messages that were missed or failed during previous consumption.

### Screenshots
- [Main Menu](/screenshots/main_menu.png)
- [Scraper Mode](/screenshots/kinesis_menu.png)
  - [Scraping Progress](/screenshots/scraping_process.png)
  - [Scraped Message](/screenshots/scraped_event.png)
- [Lambda Replay Mode](/screenshots/lambda_menu.png)

### Advanced Configuration

The Kinesis-SLR supports extensive configurable permutations and options that are not available via the official [Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html) (KCL):
- [Kinesis Scraper Configuration](config-kinesis_scraper.yaml)
  - Begin scraping options *(existing Amazon KCL [get_shard_iterator()](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis/client/get_shard_iterator.html) options)*:
    - AFTER_SEQUENCE_NUMBER
    - TRIM_HORIZON
    - LATEST
    - AT_TIMESTAMP
  - Stop Scraping options *(unique to Kinesis-SLR)*
    - TOTAL_RECORDS_PER_SHARD 
    - AT_SEQUENCE_NUMBER 
    - AFTER_SEQUENCE_NUMBER 
    - BEFORE_SEQUENCE_NUMBER 
    - AT_TIMESTAMP 
    - BEFORE_TIMESTAMP 
    - AFTER_TIMESTAMP 
    - LATEST
  - poll_batch_size *(unique to Kinesis-SLR)*
  - poll_delay *(unique to Kinesis-SLR)*
  - max_empty_polls *(unique to Kinesis-SLR)*
- [Lambda Replay Configuration](config-lambda_replay.yaml) *(unique to Kinesis-SLR)*
  - batch_size
  - local_dlq
  - local_dlq_fullevent
  - retry_attempts
  - bisect_on_error

## Functionality

Kinesis-SLR has two main components:
### Kinesis Scraper

The Kinesis-SLR scraper component reads messages from an Amazon Kinesis stream and stores them locally to disk. You can specify various criteria to determine which messages are read, such as a timestamp or sequence number. You can also read all messages up to the end of the stream.
### Lambda Replay

The Kinesis-SLR replay component reads messages from disk and replays them back to a Lambda function in configurable batches, mimicking the AWS Kinesis infrastructure. This allows you to reprocess messages that were previously missed or failed during consumption.

## Why Kinesis-SLR?

The current tooling for Amazon Kinesis does not offer many of the [advanced configuration](#advanced-configuration) options to allow for arbitrary sections of a stream to be read, then re-processed/replayed to the appropriate Lambda in various different Kinesis integrated trigger configurations. The Kinesis-SLR fills this gap by providing a single, flexible and customizable tool to read and replay messages from an Amazon Kinesis stream to your Lambdas in a manner that replicates how the messages would have been received during their normal running operation. It is especially useful for disaster recovery situations where message replays are needed.

## Getting Started

1. To get started with Kinesis-SLR, clone the repository and install the required dependencies:

```
$ git clone https://github.com/thomasfeduk/kinesis-slr.git
$ cd kinesis-slr
$ pip install -r requirements.txt
```

2. Configure your desired settings for the scraper or Lambda configuration files sepending on which mode you wish to use.
- [config-kinesis_scraper.yaml](/config-kinesis_scraper.yaml)
- [config-lambda_replay.yaml](/config-lambda_replay.yaml)

3. Then simply execute **run.py** to load the [main menu](/screenshots/main_menu.png):
```
python run.py
```

## Running unit tests

To run the unit tests, install **unittest** or **pytest** and run the following command:
```
python -m unittest
```
### License

Kinesis-SLR is licensed under the GPL 3 License.