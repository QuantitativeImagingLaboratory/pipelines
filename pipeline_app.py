from argparse import ArgumentParser
import os
import time

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('commands', nargs='*')

    args = parser.parse_args()

    print("Sleeping for 10 seconds")
    time.sleep(10)

    for k in args.commands:
        os.system(k + " &")

    print("Sleeping forever")
    while True:
        time.sleep(1000)
