import os
from typing import Callable

import boto3
import pytest

from xoto3.dynamodb.put import PutItem
from xoto3.dynamodb.types import InputItem, TableResource
from xoto3.dynamodb.utils.table import extract_key_from_item


def make_pytest_put_fixture_for_table(table_callable: Callable[[], TableResource]):
    @pytest.fixture
    def put_item_fixture():
        table = table_callable()
        keys_put = list()

        def _put_item(item: InputItem):
            keys_put.append(extract_key_from_item(table, item))
            PutItem(table, item)

        yield _put_item

        for key in keys_put:
            table.delete_item(Key=key)

    return put_item_fixture


XOTO3_INTEGRATION_TEST_ID_TABLE_NAME = os.environ.get(
    "XOTO3_INTEGRATION_TEST_DYNAMODB_ID_TABLE_NAME"
)


def get_integration_test_id_table():
    if XOTO3_INTEGRATION_TEST_ID_TABLE_NAME:
        table = boto3.resource("dynamodb").Table(XOTO3_INTEGRATION_TEST_ID_TABLE_NAME)
        if table.name:
            return table
    pytest.skip("No integration id table was defined")
    return None


integration_test_id_table_put = make_pytest_put_fixture_for_table(get_integration_test_id_table)
integration_test_id_table = pytest.fixture("module")(get_integration_test_id_table)
