#!/usr/bin/env python
import argparse
import getpass
import logging
from time import sleep

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ddb = boto3.client("dynamodb")

PK_NAME = "id"
GSI_NAME = "gsi_hash"


def _table_exists(table_name: str) -> bool:
    try:
        ddb.describe_table(TableName=table_name)
    except ddb.exceptions.ResourceNotFoundException:
        return False

    return True


def main(table_name: str, recreate=False):
    if _table_exists(table_name):
        logger.info("Table %s already exists", table_name)
        if recreate:
            logger.info("Parameter --recreate passed, deleting table and re-creating...")
            ddb.delete_table(TableName=table_name)
            while True:
                try:
                    status = ddb.describe_table(TableName=table_name)["Table"]["TableStatus"]
                    logger.info("Current table status: %s", status)
                except ddb.exceptions.ResourceNotFoundException:
                    logger.info("Table successfully deleted.")
                    break
                else:
                    sleep(3)

        else:
            logger.warning("--recreate not passed. Keeping existing table, nothing left to do.")
            return

    logger.info("Creating table %s", table_name)
    ddb.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": PK_NAME, "AttributeType": "S"},
            {"AttributeName": GSI_NAME, "AttributeType": "S"},
        ],
        KeySchema=[{"AttributeName": PK_NAME, "KeyType": "HASH"}],
        GlobalSecondaryIndexes=[
            {
                "IndexName": GSI_NAME,
                "KeySchema": [{"AttributeName": GSI_NAME, "KeyType": "HASH"},],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    while True:
        status = ddb.describe_table(TableName=table_name)["Table"]["TableStatus"]
        if status == "ACTIVE":
            logger.info("Finished table creation")
            break
        else:
            logger.info("Awaiting table creation. Current status: '%s'", status)
            sleep(3)

    logger.info("Success!\n\n")
    print("Paste the following lines into your terminal or add to your shell .rc file:\n")
    print(f"export XOTO3_INTEGRATION_TEST_DYNAMODB_ID_TABLE_NAME='{table_name}'")
    print(f"export XOTO3_INTEGRATION_TEST_NO_RANGE_KEY_INDEX_HASH_KEY='{GSI_NAME}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table-suffix", default=getpass.getuser())
    parser.add_argument("--recreate", action="store_const", const=True)

    args = parser.parse_args()

    _table_name = f"xoto3-integration-{args.table_suffix}"
    main(_table_name, args.recreate)
