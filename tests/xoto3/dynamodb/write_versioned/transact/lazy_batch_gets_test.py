from xoto3.dynamodb.write_versioned import (
    ItemTable,
    VersionedTransaction,
    versioned_transact_write_items,
)


def test_batch_get_lazy_load():
    """Tests that multiple serial `get`s perform only a single actual call
    to batch_get"""
    t = VersionedTransaction(dict())
    table_a = ItemTable("a")
    table_b = ItemTable("b")

    a1_k = dict(id="a1")
    a2_k = dict(id="a2")
    b1_k = dict(id="b1")

    a3_k = dict(id="a3")

    def triple_get(t: VersionedTransaction) -> VersionedTransaction:
        a1 = table_a.get(a1_k)(t)
        b1 = table_b.get(b1_k)(t)
        a2 = table_a.get(a2_k)(t)
        # all three of the above gets will be performed
        # together as a single call to batch_get.
        return table_a.put(dict(a3_k, items=[a1, b1, a2]))(t)

    calls = 0
    a1 = dict(a1_k, i=6)
    a2 = dict(a2_k, i=8)
    b1 = dict(b1_k, j=4)

    def batch_get(item_keys_by_table_name):
        if not item_keys_by_table_name:
            return dict()
        nonlocal calls
        calls += 1
        return dict(a=[a1, a2], b=[b1])

    t = versioned_transact_write_items(
        triple_get,
        batch_get_item=batch_get,  # type: ignore
        transact_write_items=lambda **_kw: None,
    )

    assert calls == 1
    assert table_a.require(a3_k)(t)["items"] == [a1, b1, a2]
