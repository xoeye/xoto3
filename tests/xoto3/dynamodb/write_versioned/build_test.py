import pytest

from xoto3.dynamodb.exceptions import ItemNotFoundException
from xoto3.dynamodb.write_versioned import delete, get, put, require
from xoto3.dynamodb.write_versioned.errors import ItemUndefinedException, TableSchemaUnknownError
from xoto3.dynamodb.write_versioned.keys import hashable_key, key_from_item
from xoto3.dynamodb.write_versioned.prepare import items_and_keys_to_clean_table_data
from xoto3.dynamodb.write_versioned.types import VersionedTransaction as VT


def build_items(*items_in, key_attributes=("id",)):
    items = dict()
    for item in items_in:
        items[hashable_key(key_from_item(key_attributes, item))] = item
    return items


def test_get_and_require():
    item_a = dict(id="1", val="a")
    item_b = dict(id="2", val="b")
    vt = VT(
        tables=dict(
            table1=items_and_keys_to_clean_table_data(("id",), [dict(id="no")], [item_a, item_b],)
        )
    )

    gotten = get(vt, "table1", dict(id="1"), nicename="Test",)
    assert gotten == item_a
    assert gotten is not item_a  # copied
    assert get(vt, "table1", dict(id="1"), copy=False) is item_a

    assert require(vt, "table1", dict(id="1")) == item_a

    with pytest.raises(ItemNotFoundException):
        require(vt, "table1", dict(id="no"))

    with pytest.raises(ItemUndefinedException):
        get(vt, "table2", dict(id="1"),) == dict(id="1", val="a")

    with pytest.raises(ItemUndefinedException):
        get(vt, "table1", dict(id="3"))


def test_puts_and_deletes():
    vt = VT(
        tables=dict(
            table1=items_and_keys_to_clean_table_data(("id",), tuple(), [dict(id="a", val=2)],),
            table2=items_and_keys_to_clean_table_data(
                ("id",), tuple(), [dict(id="a", val=7), dict(id="h", val="s")]
            ),
        )
    )

    with pytest.raises(TableSchemaUnknownError):
        put(vt, "table3", dict(id="seven", val="whatever"))

    put(vt, "table1", dict(id="b", val="hey"))
    # we know the table schema for this one, so we optimistically allow the put

    out_vt = put(vt, "table1", dict(id="a", val=3))
    assert get(out_vt, "table1", dict(id="a")) == dict(id="a", val=3)
    assert get(vt, "table1", dict(id="a")) == dict(id="a", val=2)  # original tx object unaffected
    assert get(out_vt, "table2", dict(id="a")) == dict(
        id="a", val=7
    )  # item in other table is unaffected
    assert get(delete(out_vt, "table1", dict(id="a")), "table1", dict(id="a")) is None
    assert get(delete(vt, "table2", dict(id="a")), "table2", dict(id="a")) is None
