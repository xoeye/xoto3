import os
from logging import getLogger
from typing import Callable, Iterator, Optional, Union

import boto3
import pytest

from xoto3.dynamodb.put import PutItem
from xoto3.dynamodb.types import InputItem, Item, ItemKey, TableResource
from xoto3.dynamodb.utils.table import extract_key_from_item

logger = getLogger(__name__)


TableCallable = Callable[[], TableResource]
AddKeyToCleaner = Callable[[ItemKey], None]
ItemCleanerGenerator = Iterator[AddKeyToCleaner]


def make_table_cleaner(table_callable: TableCallable) -> ItemCleanerGenerator:
    logger.info("Making table cleaner")
    keys_to_delete = list()

    def add_key_or_item_to_delete(key_or_item: Union[Item, ItemKey]) -> None:
        keys_to_delete.append(extract_key_from_item(table_callable(), key_or_item))

    yield add_key_or_item_to_delete

    for key in keys_to_delete:
        try:
            table_callable().delete_item(Key=key)
        except:  # noqa
            logger.exception(f"Failed to delete {key}")


def make_table_putter(
    table_callable: TableCallable, cleaner_generator: Optional[ItemCleanerGenerator] = None,
):
    def put_item_fixture():
        cleaner_yielder = cleaner_generator or make_table_cleaner(table_callable)
        assert cleaner_yielder is not None
        add_key_or_item_to_cleaner = next(cleaner_yielder)

        def _put_item(item: InputItem) -> Item:
            add_key_or_item_to_cleaner(item)
            logger.info("trying to put item", item=item)
            PutItem(table_callable(), item)
            return dict(item)

        yield _put_item

        # this consumes the cleaner yielder, which will force cleanup
        _ = [i for i in cleaner_yielder]

    return put_item_fixture


XOTO3_INTEGRATION_TEST_ID_TABLE_NAME = os.environ.get(
    "XOTO3_INTEGRATION_TEST_DYNAMODB_ID_TABLE_NAME"
)
XOTO3_INTEGRATION_TEST_NO_RANGE_KEY_INDEX_HASH_KEY = os.environ.get(
    "XOTO3_INTEGRATION_TEST_NO_RANGE_KEY_INDEX_HASH_KEY"
)


def get_integration_test_id_table():
    if XOTO3_INTEGRATION_TEST_ID_TABLE_NAME:
        table = boto3.resource("dynamodb").Table(XOTO3_INTEGRATION_TEST_ID_TABLE_NAME)
        if table.name:
            return table
    pytest.skip("No integration id table was defined")
    return None


integration_test_id_table_put = pytest.fixture(make_table_putter(get_integration_test_id_table))


def get_integration_test_no_range_key_index_hash_key():
    if not XOTO3_INTEGRATION_TEST_NO_RANGE_KEY_INDEX_HASH_KEY:
        pytest.skip("No integration test hash key was defined")
    return XOTO3_INTEGRATION_TEST_NO_RANGE_KEY_INDEX_HASH_KEY


@pytest.fixture
def integration_test_id_table_cleaner():
    table_cleaner = make_table_cleaner(get_integration_test_id_table)
    yield next(table_cleaner)
    _ = [i for i in table_cleaner]


integration_test_id_table = pytest.fixture("module")(get_integration_test_id_table)
integration_test_no_range_index_hash_key = pytest.fixture("module")(
    get_integration_test_no_range_key_index_hash_key
)
