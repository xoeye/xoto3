#!/usr/bin/env python
"""Watch a DynamoDB stream for creations, updates, and deletions.

By default only prints the primary key.
"""
import argparse
from typing import Callable

import boto3

from xoto3.dynamodb.streams.consume import process_latest_from_stream
from xoto3.dynamodb.streams.records import old_and_new_items_from_stream_event_record
from xoto3.dynamodb.utils.index import hash_key_name, range_key_name


def make_accept_stream_item_for_table(item_slicer: Callable[[dict], str]):
    def accept_stream_item(record: dict):
        old, new = old_and_new_items_from_stream_event_record(record)
        if not old:
            print(f"New item: {item_slicer(new)}")
        elif not new:
            print(f"Deleted item {item_slicer(old)}")
        else:
            print(f"Updated item; OLD: {item_slicer(old)} NEW: {item_slicer(new)}")

    return accept_stream_item


def make_key_slicer(table):
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

    return extract_primary_key


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table_name")
    parser.add_argument(
        "--attribute-names", help="Any attributes other than the key to print", nargs="*"
    )
    args = parser.parse_args()

    DDB_RES = boto3.resource("dynamodb")

    DDB_STREAMS_CLIENT = boto3.client("dynamodbstreams")

    table = DDB_RES.Table(args.table_name)

    if args.attribute_names:
        key_slicer = make_key_slicer(table)

        def item_slicer(item: dict):
            return {
                **key_slicer(item),
                **{
                    attr_name: item[attr_name]
                    for attr_name in args.attribute_names
                    if attr_name in item
                },
            }

    else:
        item_slicer = make_key_slicer(table)

    try:
        t, _kill = process_latest_from_stream(
            DDB_STREAMS_CLIENT,
            table.latest_stream_arn,
            make_accept_stream_item_for_table(item_slicer),
        )
        t.join()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
