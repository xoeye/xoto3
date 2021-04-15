from random import randint

import pytest

from xoto3.dynamodb.exceptions import ItemNotFoundException
from xoto3.dynamodb.write_versioned import (
    ItemTable,
    VersionedTransaction,
    create_or_update,
    update_existing,
    update_if_exists,
    versioned_transact_write_items,
)
from xoto3.dynamodb.write_versioned.types import KeyAttributeType

from .conftest import mock_next_run


def _randkey():
    return dict(id="".join((str(randint(0, 9)) for i in range(30))))


def test_single_table_helpers(integration_test_id_table):
    itable = ItemTable(integration_test_id_table.name, item_name="Steve")

    rand_key = _randkey()
    # random because it's actually writing to the table

    def transact(vt):
        vt = itable.put(vt, dict(rand_key, foo="bar"))
        vt = itable.delete(vt, rand_key)
        return vt

    out = versioned_transact_write_items(transact, {integration_test_id_table.name: [rand_key]})
    assert None is itable.get(out, rand_key)


def _fake_table(
    table_name: str, *key_attrs: KeyAttributeType,
):
    table = ItemTable(table_name)
    vt = VersionedTransaction(dict())
    vt = table.define(vt, *key_attrs)
    return vt, table


def _update(key, val):
    def _(it):
        it[key] = val
        return it

    return _


def test_api2_update_if_exists():
    """This test is long because of how much effort it takes to set up a fake table"""
    vt, table = _fake_table(lambda: "steve", "id")  # type: ignore

    key = dict(id="exists")
    vt = table.hypothesize(vt, key, dict(key, foo="bar"))

    vt = versioned_transact_write_items(
        update_if_exists(table.get, table.put, _update("foo", "biz"), key), **mock_next_run(vt),
    )

    assert table.require(vt, key)["foo"] == "biz"

    bad_key = dict(id="notexists")
    vt = versioned_transact_write_items(
        update_if_exists(table.get, table.put, _update("foo", "zap"), bad_key),
        dict(steve=[key]),
        **mock_next_run(vt),
    )
    assert table.require(vt, key)["foo"] == "biz"  # the same
    assert None is table.get(vt, bad_key)


def test_api2_update_existing():
    vt, table = _fake_table("steve", "id")
    key = dict(id="yo")
    vt = table.hypothesize(vt, key, dict(key, foo="blah"))

    vt = versioned_transact_write_items(
        update_existing(table.require, table.put, _update("foo", "zot"), key), **mock_next_run(vt)
    )
    assert table.require(vt, key)["foo"] == "zot"

    with pytest.raises(ItemNotFoundException):
        versioned_transact_write_items(
            update_existing(table.require, table.put, _update("foo", "oi"), dict(id="no"),),
            **mock_next_run(vt),
        )


def test_api2_create_or_update():
    vt, table = _fake_table("steve", "id")
    key = dict(id="yolo")
    vt = versioned_transact_write_items(
        create_or_update(table.get, table.put, lambda x: dict(key, foo=1), key),
        **mock_next_run(vt),
    )
    assert table.require(vt, key)["foo"] == 1
