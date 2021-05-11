#!/usr/bin/env python
"""This is mostly just a silly proof-of-concept"""
import json
import threading
from functools import partial

import boto3

from xoto3.cloudwatch.logs import funnel_latest_from_log_group
from xoto3.multicast import LazyMulticast

CLOUDWATCH_LOGS = LazyMulticast(partial(funnel_latest_from_log_group, boto3.client("logs")))


def write_log_events_to_file(log_group_name: str, filename: str):
    with open(filename, "w") as outf:
        with CLOUDWATCH_LOGS(log_group_name) as log_events:
            for event in log_events:
                outf.write(json.dumps(event) + "\n")


def main():
    while True:
        log_group_name = input("Log Group Name: ")
        if not log_group_name:
            continue
        try:
            while True:
                output_filename = input("output filename: ")
                if not output_filename:
                    continue
                t = threading.Thread(
                    target=write_log_events_to_file,
                    args=(log_group_name, output_filename),
                    daemon=True,
                )
                t.start()
                break
        except KeyboardInterrupt:
            print("\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
