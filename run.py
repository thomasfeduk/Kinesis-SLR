from includes.debug import *
from art import tprint
import yaml
import os
import textwrap


def scrape_menu():
    clear_screen()
    display_logo()
    die('scrape menu')

def replay_menu():
    clear_screen()
    display_logo()
    die('replay menu')


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
    # with open("config-kinesis_scraper.yaml", "r") as f:
    #     config = yaml.safe_load(f)
    # display_yaml(config)

    print("\n\n")
    print_wrapped("Kinesis-SLR is a tool designed for reading and replaying messages from an Amazon Kinesis stream to "
                  "local disk, then replaying those messages to one or more Lambdas. It was made to assist in "
                  "disaster recovery situations where Lambdas may have missed consuming messages, or message "
                  "processing failed and need to be re-attempted outside of the simple ranges provided by the AWS "
                  "infrastructure. It allows you to connect to any point in the stream, read messages to disk "
                  "locally, and then replay them back to a Lambda function in batches, mimicking the AWS "
                  "Kinesis>Lambda native integration (including retry handling and bisect/bisectBatchOnFunctionError) "
                  "and incorporates a local file-based DLQ to write failed messages.. With Kinesis-SLR, "
                  "you can recover and reprocess missed or failed messages during previous consumption.")
    print("Kinesis-SLR has two main components: the Scraper and the Lambda Replay.")
    print("\n\n")
    print_wrapped("The Scraper reads messages from an Amazon Kinesis stream between any two points within the stream and writes them locally to disk.")
    print("\n\n")
    print_wrapped("The lambda replay reads messages from disk and replays them back to a Lambda function in batches, ")
    print("\n\n")
    print_wrapped("This tool is designed to fill the gap in the current AWS tooling such as KCL (Kinesis Client Library), by providing a small flexible way to scrape and replay arbitrary ranges of messages in a stream.")
    print("\n\n")

    print("\nWhat would you like to do?")
    print("1. Open Kinesis Scraper Menu")
    print("2. Open Lambda Re-play Menu")
    print("3. Quit")
    choice = input("> ")
    os.system("cls" if os.name == "nt" else "clear")
    if choice == "1":
        scrape_menu()
    elif choice == "2":
        replay_menu()
    else:
        print("Exiting...")


if __name__ == "__main__":
    # scrape.main_kinesis()
    # scrape.main_lambda()

    main()
