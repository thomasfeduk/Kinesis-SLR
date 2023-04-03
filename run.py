from includes.debug import *
from art import tprint
import yaml
import os


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


def main():
    clear_screen()
    display_logo()
    with open("config-kinesis_scraper.yaml", "r") as f:
        config = yaml.safe_load(f)
    display_yaml(config)

    print("\nChoose an option:")
    print("1. Open Scraper Menu")
    print("2. Open Replay Menu")
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
