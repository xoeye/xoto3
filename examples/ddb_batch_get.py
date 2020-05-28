#!/usr/bin/env python
"""This little example script only supports BatchGet on tables with
simple (non-composite) keys (i.e., the base index is HASH only, not
HASH+RANGE) for the sake of keeping the CLI manageable.

However, BatchGetItem itself supports HASH+RANGE keys just fine, where
a key would look something like `dict(activity_group='XOi',
id='job-1234')`.
"""
import argparse
from pprint import pprint

import boto3

from xoto3.dynamodb.batch_get import BatchGetItem, items_only


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table_name")
    parser.add_argument("ids", nargs="+")
    parser.add_argument("--key-name", default="id", help="the name of your hash key attribute")
    args = parser.parse_args()

    table = boto3.resource("dynamodb").Table(args.table_name)

    for item in items_only(  # we don't care about keys, nor items that aren't found
        BatchGetItem(
            table,
            # make a proper ItemKey (a dict) for each of the things you're looking to get
            ({args.key_name: id} for id in args.ids),
            # this is a memory-efficient generator but you can pass a list or tuple of dicts too
        )
    ):
        pprint(item)


if __name__ == "__main__":
    main()
