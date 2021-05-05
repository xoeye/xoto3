#!/usr/bin/env python
import argparse
from datetime import datetime, timezone

import boto3

from xoto3.cloudwatch.logs import yield_filtered_log_events


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_group_name")
    parser.add_argument("--filter-pattern", "-f", default="")
    args = parser.parse_args()

    cw_client = boto3.client("logs")

    start_time = datetime.now(timezone.utc)

    for log_event in yield_filtered_log_events(
        cw_client, args.log_group_name, start_time, args.filter_pattern
    ):
        print(log_event["message"])


if __name__ == "__main__":
    main()
