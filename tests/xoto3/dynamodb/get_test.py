import random

import pytest

from xoto3.dynamodb.exceptions import ItemNotFoundException, get_item_exception_type
from xoto3.dynamodb.get import (
    GetItem,
    strongly_consistent_get_item,
    strongly_consistent_get_item_if_exists,
)


def test_get_not_exists(integration_test_id_table):
    random_key = dict(id=str(random.randint(9999999, 999999999999999999999)))
    exc_type = get_item_exception_type("TestItem2", ItemNotFoundException)
    with pytest.raises(exc_type):
        GetItem(integration_test_id_table, random_key, nicename="TestItem2")

    with pytest.raises(ItemNotFoundException):
        GetItem(integration_test_id_table, random_key, nicename="")

    assert dict() == strongly_consistent_get_item_if_exists(integration_test_id_table, random_key)


def test_get_exists(integration_test_id_table, integration_test_id_table_put):
    # setup
    random_key = dict(id=str(random.randint(9999999, 999999999999999999999)))
    item = dict(random_key, test_item_please_ignore=True)
    integration_test_id_table_put(item)

    assert item == strongly_consistent_get_item(
        integration_test_id_table, random_key, nicename="TestItem2"
    )

    assert item == strongly_consistent_get_item_if_exists(
        integration_test_id_table, random_key, nicename="TestItem2"
    )
