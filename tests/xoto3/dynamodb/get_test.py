import random

import pytest

from xoto3.dynamodb.exceptions import ItemNotFoundException, get_item_exception_type
from xoto3.dynamodb.get import (
    GetItem,
    GetItem_kwargs,
    retry_notfound_consistent_read,
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


def test_retry_get_with_consistent_read_if_it_exists():

    the_item = dict(id="foo")

    def _fake_get(**kw):
        if not kw.get("ConsistentRead"):
            raise ItemNotFoundException("ouch")
        return the_item

    test_get = retry_notfound_consistent_read(_fake_get)

    assert test_get() == the_item


def test_retry_get_with_consistent_read_if_it_does_not_exist():
    calls = 0

    def _fake_get(**kw):
        nonlocal calls
        calls += 1
        if not kw.get("ConsistentRead"):
            raise ItemNotFoundException("ouch")
        raise ItemNotFoundException("definitely does not exist")

    test_get = retry_notfound_consistent_read(_fake_get)

    with pytest.raises(ItemNotFoundException):
        test_get()

    assert calls == 2


def test_dont_retry_get_with_consistent_read_if_it_was_already_consistent():
    calls = 0

    def _fake_get(**kw):
        nonlocal calls
        calls += 1
        if not kw.get("ConsistentRead"):
            raise ItemNotFoundException("ouch")
        raise ItemNotFoundException("definitely does not exist")

    test_get = retry_notfound_consistent_read(_fake_get)

    with pytest.raises(ItemNotFoundException):
        test_get(ConsistentRead=True)

    assert calls == 1


def test_consistent_read_via_kwargs(integration_test_id_table, integration_test_id_table_put):
    item_key = dict(id="item-will-not-immediately-exist")
    item = dict(item_key, val="felicity")

    integration_test_id_table_put(item)

    with GetItem_kwargs.set_default(dict(ConsistentRead=True)):
        assert item == GetItem(integration_test_id_table, item_key)
