import pytest

from xoto3.dynamodb.put import PutItem
from xoto3.dynamodb.types import TableResource, InputItem
from xoto3.dynamodb.utils.table import extract_key_from_item


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
