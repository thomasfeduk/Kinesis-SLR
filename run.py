import scrape
from includes.debug import *
from art import tprint
import yaml
import os

def clear_screen():
        os.system('cls')
        os.system('clear')

class YamlTable:
    def __init__(self, data):
        self.data = data

    def display_table(self):
        for key, value in config.items():
            print("{:<30}{}".format(key, value))

    def prompt_user(self):
        print("\nChoose an option:")
        print("1. Option 1")
        print("2. Option 2")
        print("3. Option 3")
        choice = input("> ")
        os.system("cls" if os.name == "nt" else "clear")
        if choice == "1":
            self.function1()
        elif choice == "2":
            self.function2()
        elif choice == "3":
            self.function3()
        else:
            print("Invalid choice")

    def function1(self):
        print("Running function 1")

    def function2(self):
        print("Running function 2")

    def function3(self):
        print("Running function 3")


if __name__ == "__main__":
    # scrape.main_kinesis()
    # scrape.main_lambda()

    with open("config-kinesis_scraper.yaml", "r") as f:
        config = yaml.safe_load(f)


    table = YamlTable(config)
    table.display_table()
    table.prompt_user()


    die('end')


    clear_screen()
    lines = ''.join(['_' for _ in range(103)])
    print(f"{lines}\n\n")
    # tprint("Kinesis-SLR", font="slant", chr_ignore=True)
    # tprint("Kinesis-SLR", font="larry", chr_ignore=True)
    tprint("Kinesis-SLR", font="rounded", chr_ignore=True)
    print(lines)
    die()

    # Create a Bo
