# Kinesis-SLR

Kinesis-SLR (Kinesis Scrapper and Lambda Replay) is a Python program that facilitates reading and replaying messages from an Amazon Kinesis stream. It is designed especially for disaster recovery situations where services may have missed consuming messages or message processing failed, and replays need to occur.

With Kinesis-SLR, you can connect to any point in the stream, read messages to disk locally, and then replay them back to a Lambda function in batches. This allows you to recover and reprocess messages that were missed or failed during previous consumption.

## Functionality

Kinesis-SLR has two main components:
### Kinesis Scraper

The Kinesis-SLR scraper component reads messages from an Amazon Kinesis stream and stores them locally to disk. You can specify various criteria to determine which messages are read, such as a timestamp or sequence number. You can also read all messages up to the end of the stream.
### Lambda Replay

The Kinesis-SLR replay component reads messages from disk and replays them back to a Lambda function in batches. This allows you to reprocess messages that were previously missed or failed during consumption.
## Why Kinesis-SLR?

The current tooling for Amazon Kinesis does not allow for arbitrary sections of a stream to be read and re-processed/replayed. Kinesis-SLR fills this gap by providing a flexible and customizable way to read and replay messages from an Amazon Kinesis stream. It is especially useful for disaster recovery situations where message replays are needed.
## Getting Started

To get started with Kinesis-SLR, clone the repository and install the required dependencies:

```
$ git clone https://github.com/thomasfeduk/kinesis-slr.git
$ cd kinesis-slr
$ pip install -r requirements.txt
```

Then simply execute **run.py**:
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