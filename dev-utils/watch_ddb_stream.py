#!/usr/bin/env python
"""Watch a DynamoDB stream for creations, updates, and deletions.

By default only prints the primary key.
"""
import argparse
from typing import Callable

import boto3

from xoto3.dynamodb.streams.consume import ItemImages, make_dynamodb_stream_images_multicast
from xoto3.dynamodb.utils.index import hash_key_name, range_key_name

DYNAMODB_STREAMS = make_dynamodb_stream_images_multicast()


def make_accept_stream_images(item_slicer: Callable[[ItemImages], dict]):
    def accept_stream_item(images: ItemImages) -> None:
        old, new = images
        if not old:
            print(f"New item: {item_slicer(images)}")  # type: ignore
        elif not new:
            print(f"Deleted item {item_slicer(images)}")
        else:
            print(f"Updated item; DIFF: {item_slicer(images)}")

    return accept_stream_item


def make_key_slicer(table):
    hash_key = hash_key_name(table.key_schema)
    try:
        range_key = range_key_name(table.key_schema)
    except ValueError:
        range_key = ""

    def extract_primary_key(images: ItemImages) -> dict:
        old, new = images
        item = new or old
        assert item is not None
        key = dict()
        key[hash_key] = item[hash_key]
        if range_key:
            key[range_key] = item[range_key]
        return key

    return extract_primary_key


def make_item_slicer(key_slicer, attribute_names):
    def item_slicer(images: ItemImages) -> dict:
        old, new = images
        if not new:
            new = dict()
        if not old:
            old = dict()
        item = new or old
        key = key_slicer(images)
        diff = {name for name in (set(old) | set(new)) if old.get(name) != new.get(name)}
        return {
            **key,
            **{attr_name: item[attr_name] for attr_name in attribute_names if attr_name in item},
            **{diff_name: item.get(diff_name) for diff_name in diff},
        }

    return item_slicer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table_name")
    parser.add_argument(
        "--attribute-names",
        help="Any attributes other than the key to print on every update; space separated",
        nargs="*",
        default=list(),
    )
    args = parser.parse_args()

    DDB_RES = boto3.resource("dynamodb")

    table = DDB_RES.Table(args.table_name)

    item_slicer = make_item_slicer(make_key_slicer(table), args.attribute_names)

    try:
        accept_stream_images = make_accept_stream_images(item_slicer)
        with DYNAMODB_STREAMS(args.table_name) as table_stream:
            for images in table_stream:
                accept_stream_images(images)
    except KeyboardInterrupt:
        pass  # no noisy log - Ctrl-C for clean exit


if __name__ == "__main__":
    main()
