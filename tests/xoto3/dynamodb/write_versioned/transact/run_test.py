from typing import List, cast

import pytest
from botocore.exceptions import ClientError

from xoto3.dynamodb.write_versioned import (
    ItemKeysByTableName,
    ItemsByTableName,
    TransactionAttemptsOverrun,
    VersionedTransaction,
    delete,
    get,
    put,
    require,
    versioned_transact_write_items,
)
from xoto3.dynamodb.write_versioned.types import BatchGetItem, TransactWriteItems


def test_no_io_run():
    attempts = 0

    def batch_get_item(RequestItems: ItemKeysByTableName, **_kwargs) -> ItemsByTableName:
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

    def transact_write_items(TransactItems: List[dict], **kwargs):
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
        batch_get_item=cast(BatchGetItem, batch_get_item),
        transact_write_items=cast(TransactWriteItems, transact_write_items),
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
        batch_get_item=cast(BatchGetItem, batch_get_item),
        transact_write_items=cast(TransactWriteItems, transact_write_items),
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
            batch_get_item=cast(BatchGetItem, batch_get_item),
            transact_write_items=cast(TransactWriteItems, transact_always_cancel),
            attempts_iterator=range(4),
        )
    assert "Failed after 4 attempts" in str(e)

    def transact_resource_not_found(**_):
        raise ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "transact_write_items")

    with pytest.raises(ClientError):
        versioned_transact_write_items(
            build_transaction,
            dict(tbl1=[dict(id="a"), dict(id="f"),], tbl2=[dict(id="b"), dict(id="c"),],),
            batch_get_item=cast(BatchGetItem, batch_get_item),
            transact_write_items=transact_resource_not_found,
        )


def test_integration_test_inc_and_create_and_delete(
    integration_test_id_table, integration_test_id_table_put, integration_test_id_table_cleaner
):

    test_1_id = "versioned-transact-write-items-test-1"
    test_2_id = "versioned-transact-write-items-test-2"
    test_3_id = "versioned-transact-write-items-test-3"

    integration_test_id_table_put(dict(id=test_1_id, val=8))
    integration_test_id_table_put(dict(id=test_3_id, val=9))
    integration_test_id_table_cleaner(dict(id=test_2_id))

    def inc_and_create_and_delete(tx: VersionedTransaction) -> VersionedTransaction:
        test1 = require(tx, integration_test_id_table.name, dict(id=test_1_id))
        test2 = dict(id=test_2_id, val=test1["val"])
        tx = put(tx, integration_test_id_table, test2)
        test1["val"] += 2
        tx = put(tx, integration_test_id_table.name, test1)
        tx = delete(tx, integration_test_id_table.name, dict(id=test_3_id))
        return tx

    versioned_transact_write_items(
        inc_and_create_and_delete,
        {
            integration_test_id_table.name: [
                dict(id=test_1_id),
                dict(id=test_2_id),
                dict(id=test_3_id),
            ]
        },
    )

    assert integration_test_id_table.get_item(Key=dict(id=test_1_id))["Item"]["val"] == 10
    assert integration_test_id_table.get_item(Key=dict(id=test_2_id))["Item"]["val"] == 8
    assert "Item" not in integration_test_id_table.get_item(Key=dict(id=test_3_id))


def test_integration_optimize_single_put(
    integration_test_id_table, integration_test_id_table_put, integration_test_id_table_cleaner
):
    test_id = "versioned-transact-optimize-put"

    integration_test_id_table_cleaner(dict(id=test_id))

    def create(tx: VersionedTransaction) -> VersionedTransaction:
        return put(tx, integration_test_id_table.name, dict(id=test_id, val=99))

    versioned_transact_write_items(
        create, {integration_test_id_table.name: [dict(id=test_id),]},
    )


def test_integration_optimize_single_delete(
    integration_test_id_table, integration_test_id_table_put
):
    test_id = "versioned-transact-optimize-delete"

    integration_test_id_table_put(dict(id=test_id, val=98))

    def test_delete(tx: VersionedTransaction) -> VersionedTransaction:
        return delete(tx, integration_test_id_table.name, dict(id=test_id))

    versioned_transact_write_items(
        test_delete, {integration_test_id_table.name: [dict(id=test_id),]},
    )


def test_no_op_builder():
    def builder(tx):
        assert False
        return tx

    versioned_transact_write_items(
        builder, dict(table1=[], table2=[]),
    )
