import os

import boto3
import pytest

from xoto3.dynamodb.put import PutItem
from xoto3.dynamodb.types import InputItem, TableResource
from xoto3.dynamodb.utils.table import extract_key_from_item


def pytest_addoption(parser):
    parser.addoption(
        "--runinteg",
        action="store_true",
        default=False,
        help="run integration tests against real infrastructure",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "integ: mark test as an integration test")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runinteg"):
        # --runinteg given in cli: do not skip integration tests
        return
    skip_integ = pytest.mark.skip(reason="need --runinteg option to run")
    for item in items:
        if "integ" in item.keywords:
            item.add_marker(skip_integ)


def make_pytest_put_fixture_for_table(table: TableResource):
    @pytest.fixture
    def put_item_fixture():
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
    pytest.skip(reason="No integration id table was defined")
    return None


integration_test_id_table_put = make_pytest_put_fixture_for_table(get_integration_test_id_table())
integration_test_id_table = pytest.fixture("module")(get_integration_test_id_table)
