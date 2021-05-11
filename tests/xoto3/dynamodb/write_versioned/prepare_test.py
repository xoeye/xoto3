import pytest

import xoto3.dynamodb.write_versioned as wv
from xoto3.dynamodb.write_versioned.prepare import (
    add_item_to_base_request,
    all_items_for_next_attempt,
    parse_batch_get_request,
)


def test_disallow_non_matching_keys():
    with pytest.raises(AssertionError):
        parse_batch_get_request(dict(tbl1=[dict(id=1), dict(other_key=2)]))


def test_deduplicate_keys():
    req = [dict(id=1), dict(id=1), dict(id=2)]

    res = parse_batch_get_request(dict(tbl1=req))
    assert res == dict(tbl1=[dict(id=1), dict(id=2)])


def test_add_item():
    tname_onto_item_keys = add_item_to_base_request(
        dict(table1=[dict(id=1)]), ("table2", dict(id=3)),
    )

    assert tname_onto_item_keys == dict(table1=[dict(id=1)], table2=[dict(id=3)],)


def test_all_items_for_next_attempt_different_key_schemas():
    FooTable = wv.ItemTable("Foo")
    BarTable = wv.ItemTable("Bar")
    BazTable = wv.ItemTable("Baz")
    vt = wv.VersionedTransaction(dict())
    vt = FooTable.define("someKey")(vt)
    vt = BarTable.define("someOtherKey")(vt)
    vt = BazTable.define("aThirdKey")(vt)
    vt = FooTable.presume(dict(someKey="foo-1"), dict(someKey="foo-1"))(vt)
    vt = BarTable.presume(dict(someOtherKey="bar-1"), dict(someOtherKey="bar-1"))(vt)
    vt = BazTable.presume(dict(aThirdKey="baz-1"), dict(aThirdKey="baz-1"))(vt)
    vt = BazTable.presume(dict(aThirdKey="baz-2"), dict(aThirdKey="baz-2"))(vt)

    result = all_items_for_next_attempt(vt)
    assert result["Foo"] == [dict(someKey="foo-1")]
    assert result["Bar"] == [dict(someOtherKey="bar-1")]
    assert dict(aThirdKey="baz-1") in result["Baz"]
    assert dict(aThirdKey="baz-2") in result["Baz"]
