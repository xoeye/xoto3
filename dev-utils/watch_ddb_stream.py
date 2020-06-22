#!/usr/bin/env python
"""Watch a DynamoDB stream for creations, updates, and deletions.

By default only prints the primary key.
"""
import argparse
import boto3

from xoto3.dynamodb.streams.records import old_and_new_items_from_stream_event_record
from xoto3.dynamodb.streams.consume import process_latest_from_stream
from xoto3.dynamodb.utils.index import hash_key_name, range_key_name


DDB_RES = boto3.resource("dynamodb")

DDB_STREAMS_CLIENT = boto3.client("dynamodbstreams")


def make_accept_stream_item_for_table(table):
    hash_key = hash_key_name(table.key_schema)
    try:
        range_key = range_key_name(table.key_schema)
    except ValueError:
        range_key = ""

    def extract_primary_key(item: dict):
        key = dict()
        key[hash_key] = item[hash_key]
        if range_key:
            key[range_key] = item[range_key]
        return key

    def accept_stream_item(record: dict):
        old, new = old_and_new_items_from_stream_event_record(record)
        if not old:
            print(f"New item {extract_primary_key(new)}")
        elif not new:
            print(f"Deleted item {extract_primary_key(old)}")
        else:
            print(f"Updated item {extract_primary_key(new)}")

    return accept_stream_item


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table_name")
    args = parser.parse_args()

    table = DDB_RES.Table(args.table_name)

    try:
        t, _kill = process_latest_from_stream(
            DDB_STREAMS_CLIENT, table.latest_stream_arn, make_accept_stream_item_for_table(table)
        )
        t.join()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
