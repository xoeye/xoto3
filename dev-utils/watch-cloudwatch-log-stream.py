#!/usr/bin/env python
"""

"""
import argparse
import sys
from datetime import datetime, timezone

import boto3

from xoto3.cloudwatch.logs import yield_filtered_log_events
from xoto3.utils.dt import iso8601strict, parse8601strict

DEFAULT_LOG_REDUCER = """
class LogReducer:
    def __init__(self):
        self.events = 0

    def __call__(self, log_event):
        self.events += 1
        msg = log_event["message"]
        if not msg.startswith("REPORT RequestId"):
            msg = msg.rstrip("\\n")
        print(msg)

    def __exit__(self):
        print(f"{self.events} events")
"""


def exec_log_reducer(s: str):
    try:
        code = open(s).read()
    except:  # noqa
        code = s
    exec(code, globals())
    return LogReducer()  # noqa


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("log_group_name")
    parser.add_argument("--filter-pattern", "-f", default="")
    parser.add_argument("--start-time", "-s", default=iso8601strict(datetime.now(timezone.utc)))
    parser.add_argument("--end-time", "-e", default="")
    parser.add_argument("--log-reducer", default=DEFAULT_LOG_REDUCER)
    args = parser.parse_args()

    cw_client = boto3.client("logs")
    start_time = parse8601strict(args.start_time, aware=True)

    events = 0

    log_reducer = exec_log_reducer(args.log_reducer)

    try:
        for log_event in yield_filtered_log_events(
            cw_client,
            args.log_group_name,
            start_time,
            args.filter_pattern,
            end_time=parse8601strict(args.end_time) if args.end_time else None,
        ):
            events += 1
            log_reducer(log_event)
    except KeyboardInterrupt:
        pass
    exit = getattr(log_reducer, "__exit__", None)
    if exit:
        exit()
    return events


if __name__ == "__main__":
    sys.exit(0 if main() else -1)
