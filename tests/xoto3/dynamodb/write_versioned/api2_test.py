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
    write_item,
)

from .conftest import mock_next_run


def _randkey():
    return dict(id="".join((str(randint(0, 9)) for i in range(30))))


def test_single_table_helpers(integration_test_id_table):
    itable = ItemTable(integration_test_id_table.name, item_name="Steve")

    rand_key = _randkey()
    # random because it's actually writing to the table

    def transact(vt):
        vt = itable.put(dict(rand_key, foo="bar"))(vt)
        vt = itable.delete(rand_key)(vt)
        return vt

    out = versioned_transact_write_items(transact, {integration_test_id_table.name: [rand_key]})
    assert None is itable.get(rand_key)(out)


def _fake_table(
    table_name: str, *key_attrs: str,
):
    table = ItemTable(table_name)
    vt = VersionedTransaction(dict())
    vt = table.define(*key_attrs)(vt)
    return vt, table


def _update(key, val):
    def _(it):
        it[key] = val
        return it

    return _


def test_api2_update_if_exists():
    vt, table = _fake_table(lambda: "steve", "id")  # type: ignore

    key = dict(id="exists")
    vt = table.presume(key, dict(key, foo="bar"))(vt)

    vt = versioned_transact_write_items(
        update_if_exists(table, _update("foo", "biz"), key), **mock_next_run(vt),
    )

    assert table.require(key)(vt)["foo"] == "biz"

    bad_key = dict(id="notexists")
    vt = versioned_transact_write_items(
        update_if_exists(table, _update("foo", "zap"), bad_key),
        dict(steve=[key]),
        **mock_next_run(vt),
    )
    assert table.require(key)(vt)["foo"] == "biz"  # the same
    assert None is table.get(bad_key)(vt)


def test_api2_update_existing():
    vt, table = _fake_table("steve", "id")
    key = dict(id="yo")
    vt = table.presume(key, dict(key, foo="blah"))(vt)

    vt = versioned_transact_write_items(
        update_existing(table, _update("foo", "zot"), key), **mock_next_run(vt)
    )
    assert table.require(key)(vt)["foo"] == "zot"

    with pytest.raises(ItemNotFoundException):
        versioned_transact_write_items(
            update_existing(table, _update("foo", "oi"), dict(id="no"),), **mock_next_run(vt),
        )


def test_api2_create_or_update():
    vt, table = _fake_table("steve", "id")
    key = dict(id="yolo")
    vt = versioned_transact_write_items(
        create_or_update(table, lambda x: dict(key, foo=1), key), **mock_next_run(vt),
    )
    assert table.require(key)(vt)["foo"] == 1


def test_api2_write_item_creates():
    vt, table = _fake_table("felicity", "id")
    key = dict(id="goodbye")
    vt = versioned_transact_write_items(
        write_item(table, lambda ox: dict(key, bar=8), key), **mock_next_run(vt),
    )
    assert table.require(key)(vt)["bar"] == 8


def test_api2_write_item_deletes():
    vt, table = _fake_table("felicity", "id")
    key = dict(id="goodbye")
    vt = table.presume(key, dict(key, baz=9))(vt)
    vt = versioned_transact_write_items(
        write_item(table, lambda x: None, key), **mock_next_run(vt),
    )
    assert table.get(key)(vt) is None
