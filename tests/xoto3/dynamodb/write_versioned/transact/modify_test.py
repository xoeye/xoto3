import pytest

from xoto3.dynamodb.write_versioned import VersionedTransaction as VT
from xoto3.dynamodb.write_versioned.modify import TableSchemaUnknownError, delete, put
from xoto3.dynamodb.write_versioned.prepare import items_and_keys_to_clean_table_data


def test_cant_delete_non_prefetched_item_without_specifying_key():

    tx = delete(VT(dict()), "table1", dict(id="whatever"))

    tx = delete(tx, "table1", dict(id="yo", value=3, full_item=True))

    with pytest.raises(TableSchemaUnknownError):
        delete(tx, "table2", dict(id=4, value=7, other_value=9))


def test_skip_put_if_object_is_identical():
    item_a = dict(id=1, val=8)
    vt = VT(tables=dict(TableA=items_and_keys_to_clean_table_data(("id",), [], [item_a])))

    vt2 = put(vt, "TableA", item_a)

    assert vt2 == vt


def test_skip_put_if_object_is_identical_2():
    item_a = dict(id=1, val=8)
    vt = VT(tables=dict(TableA=items_and_keys_to_clean_table_data(("id",), [], [item_a])))

    item_a_2 = dict(id=1, val=9)
    vt2 = put(vt, "TableA", item_a_2)

    assert vt2 is not vt

    vt3 = put(vt2, "TableA", item_a_2)

    assert vt3 is vt2
