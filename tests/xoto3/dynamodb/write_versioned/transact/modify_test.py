import pytest

from xoto3.dynamodb.write_versioned import VersionedTransaction, get, put, require
from xoto3.dynamodb.write_versioned.keys import hashable_key
from xoto3.dynamodb.write_versioned.modify import (
    TableSchemaUnknownError,
    define_table,
    delete,
    presume,
)
from xoto3.dynamodb.write_versioned.types import _TableData


def test_cant_delete_non_prefetched_item_without_specifying_key():

    tx = delete(VersionedTransaction(dict()), "table1", dict(id="whatever"))

    tx = delete(tx, "table1", dict(id="yo", value=3, full_item=True))

    with pytest.raises(TableSchemaUnknownError):
        delete(tx, "table2", dict(id=4, value=7, other_value=9))


def test_presume():
    tx = VersionedTransaction(dict())
    tx = presume(tx, "steve", dict(group="g", id="123"), None)
    assert None is get(tx, "steve", dict(group="g", id="123"))


def test_presume_already_exists():
    key = dict(id=123)
    tx = VersionedTransaction(
        dict(
            steve=_TableData(
                items={hashable_key(key): dict(key, foo=2)}, effects=dict(), key_attributes=("id",)
            )
        )
    )
    tx = presume(tx, "steve", key, None)
    assert require(tx, "steve", dict(id=123))["foo"] == 2


def test_define_table():
    tx = VersionedTransaction(dict())
    tx = define_table(tx, "BobTable", "group", "id")
    item_key = dict(group=1, id=2)
    item = dict(item_key, foo=7)
    tx = put(tx, "BobTable", item)
    assert require(tx, "BobTable", item_key) == item


def test_define_table_doesnt_redefine():
    tx = delete(VersionedTransaction(dict()), "table1", dict(id="foo"))
    same_tx = define_table(tx, "table1", "id")
    assert same_tx is tx
