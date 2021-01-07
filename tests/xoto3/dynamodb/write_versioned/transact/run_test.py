import pytest
from botocore.exceptions import ClientError

from xoto3.dynamodb.write_versioned.transact import (
    TransactionAttemptsOverrun,
    delete,
    get,
    put,
    require,
    versioned_transact_write_items,
)
from xoto3.dynamodb.write_versioned.transact.types import ItemsByTableName


def test_no_io_run():
    attempts = 0

    def batch_get_item(RequestItems, **_kwargs) -> ItemsByTableName:
        nonlocal attempts
        item_c_val = -2
        if attempts == 0:
            return dict(
                tbl1=[dict(id="a", val=2, other_attr=5), dict(id="f", val=99),],
                tbl2=[dict(id="b", val=1,), dict(id="c", val=item_c_val),],
            )
        item_c_val = 8
        return dict(
            tbl1=[dict(id="a", val=2, other_attr=5), dict(id="f", val=99),],
            tbl2=[dict(id="b", val=1,), dict(id="c", val=item_c_val),],
        )

    def transact_write_items(TransactItems, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ClientError(
                {"Error": {"Code": "TransactionCanceledException"}}, "transact_write_items"
            )
        # otherwise "succeed"

    def build_transaction(tract):
        tract = put(
            tract,
            "tbl1",
            dict(
                id="a",
                val=require(tract, "tbl1", dict(id="a"))["val"]
                + require(tract, "tbl2", dict(id="c"))["val"],
            ),
        )
        tract = delete(tract, "tbl2", dict(id="c"))
        return put(tract, "tbl2", dict(id="b", val=4))

    result = versioned_transact_write_items(
        build_transaction,
        dict(tbl1=[dict(id="a"), dict(id="f"),], tbl2=[dict(id="b"), dict(id="c"),],),
        batch_get_item=batch_get_item,
        transact_write_items=transact_write_items,
    )
    assert require(result, "tbl1", dict(id="a"))["val"] == 10
    assert require(result, "tbl2", dict(id="b"))["val"] == 4
    assert get(result, "tbl2", dict(id="c")) is None
    assert require(result, "tbl1", dict(id="f"))["val"] == 99

    assert attempts == 2

    # test no-op early return
    versioned_transact_write_items(
        lambda _: _,
        dict(tbl1=[dict(id="a"), dict(id="f"),], tbl2=[dict(id="b"), dict(id="c"),],),
        batch_get_item=batch_get_item,
        transact_write_items=transact_write_items,
    )

    assert attempts == 2

    # we eventually raise after lots of failures
    def transact_always_cancel(TransactItems):
        raise ClientError(
            {"Error": {"Code": "TransactionCanceledException"}}, "transact_write_items"
        )

    with pytest.raises(TransactionAttemptsOverrun) as e:
        versioned_transact_write_items(
            build_transaction,
            dict(tbl1=[dict(id="a"), dict(id="f"),], tbl2=[dict(id="b"), dict(id="c"),],),
            batch_get_item=batch_get_item,
            transact_write_items=transact_always_cancel,
            attempts_iterator=range(4),
        )
    assert "Failed after 4 attempts" in str(e)

    def transact_resource_not_found(**_):
        raise ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "transact_write_items")

    with pytest.raises(ClientError):
        versioned_transact_write_items(
            build_transaction,
            dict(tbl1=[dict(id="a"), dict(id="f"),], tbl2=[dict(id="b"), dict(id="c"),],),
            batch_get_item=batch_get_item,
            transact_write_items=transact_resource_not_found,
        )
