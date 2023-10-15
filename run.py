from art import tprint
import boto3
import yaml
import os
import textwrap
import logging
from includes import common
from includes import kinesis_client as kinesis
from includes import lambda_client

# Initialize logger
logging.basicConfig()


def replay_menu():
    clear_screen()
    display_logo()
    hsize = 60
    print("\n")
    print(f"{'-' * hsize}")
    print("Replay Configuration:")
    print(f"{'-' * hsize}")
    with open("config-lambda_replay.yaml", "r") as f:
        config = yaml.safe_load(f)
    display_yaml(config)
    print(f"{'-' * hsize}")
    print("\n")
    print(f'Please review the above carefully. To begin replaying now, type: "replay {config["function_name"]}"\n')
    choice = input("Enter Option, or 'q' to quit: ")
    if choice == f'replay {config["function_name"]}':
        begin_replaying()
    elif choice == "q":
        print("Exiting...")
        exit(0)
    else:
        print('Invalid choice. Press any key to restart....')
        input()
        replay_menu()


def begin_replaying():
    config_yaml = common.read_config('config-lambda_replay.yaml')
    lambda_config = lambda_client.ClientConfig(config_yaml, boto3.client('lambda', "us-east-1"))
    client = lambda_client.Client(lambda_config)
    client.begin_processing()


def begin_scraping():
    config_yaml = common.read_config('config-kinesis_scraper.yaml')
    kinesis_config = kinesis.ClientConfig(config_yaml, boto3.client('kinesis', config_yaml['region_name']))
    kinesis_client = kinesis.Client(kinesis_config)
    kinesis_client.begin_scraping()


def scrape_menu():
    clear_screen()
    display_logo()
    hsize = 60
    print("\n")
    print(f"{'-' * hsize}")
    print("Scraper Configuration:")
    print(f"{'-' * hsize}")
    with open("config-kinesis_scraper.yaml", "r") as f:
        config = yaml.safe_load(f)
    display_yaml(config)
    print(f"{'-' * hsize}")
    print("\n")
    print(f'Please review the above carefully. To begin scraping now, type: "scrape {config["stream_name"]}"\n')
    choice = input("Enter Option, or 'q' to quit: ")
    if choice == f'scrape {config["stream_name"]}':
        begin_scraping()
    elif choice == "q":
        print("Exiting...")
        exit(0)
    else:
        print('Invalid choice. Press any key to restart....')
        input()
        scrape_menu()


def clear_screen():
    os.system('cls')
    os.system('clear')


def display_yaml(config):
    for key, value in config.items():
        print("{:<30}{}".format(key, value))


def display_logo():
    lines = ''.join(['_' for _ in range(103)])
    print(f"{lines}\n\n")
    # tprint("Kinesis-SLR", font="slant", chr_ignore=True)
    # tprint("Kinesis-SLR", font="larry", chr_ignore=True)
    tprint("Kinesis-SLR", font="rounded", chr_ignore=True)
    print(lines)


def print_wrapped(text: str, *, width: int = 100):
    wrapped_text = textwrap.wrap(text, width=width)
    for line in wrapped_text:
        print(line)


def main():
    clear_screen()
    display_logo()

    print("\n\n")
    print_wrapped("The Kinesis-SLR is designed to fill the gap in the current AWS tooling such as KCL (Kinesis Client "
                  "Library), by providing a small flexible, Python-only way of sraping arbitrary ranges of messages "
                  "from a stream to disk, then replay those messages back to one or more Lambdas.")
    print("\n")
    print_wrapped("It was made to especially facilitate disaster recovery situations where Lambdas may have missed "
                  "consuming messages, or message processing failed and need to be re-attempted outside of the simple "
                  "options provided by the AWS infrastructure. The Kinesis-SLR can connect to any point in the "
                  "stream, scrape those messages until a specified point in the stream while writing them to disk "
                  "locally as JSON files. Then, the Kinesis-SLR can replay them those messages to a Lambda function "
                  "in batches (mimicking the AWS Kinesis>Lambda native integration handling, including retries, "
                  "bisectBatchOnFunctionError and incorporates a local file-based DLQ to write failed messages).")
    print("\n")
    print("\nWhat would you like to do?\n")
    print("1. Open Kinesis Scraper Menu")
    print("2. Open Lambda Replay Menu")
    print("3. Quit\n")
    choice = input("Enter Option #: ")
    os.system("cls" if os.name == "nt" else "clear")
    if choice == "1":
        scrape_menu()
    elif choice == "2":
        replay_menu()
    else:
        print("Exiting...")


if __name__ == "__main__":
    main()
