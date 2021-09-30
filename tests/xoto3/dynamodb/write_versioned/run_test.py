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
    presume,
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
                {
                    "Error": {
                        "Code": "TransactionCanceledException",
                        "CancellationReasons": [
                            dict(Code="ConditionalCheckFailed"),
                            dict(Code="ProvisionedThroughputExceeded"),
                        ],
                    }
                },
                "transact_write_items",
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
            {"Error": {"Code": "TransactionInProgressException"}}, "transact_write_items"
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


def test_presumed_items_get_returned_even_without_effects_being_executed():
    def noop(vt: VersionedTransaction) -> VersionedTransaction:
        return presume(vt, "tableA", dict(id="presume_me"), None)

    result_vt = versioned_transact_write_items(noop)
    assert get(result_vt, "tableA", dict(id="presume_me")) is None


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
    ran = False

    def builder(tx):
        nonlocal ran
        ran = True
        # transaction does actually run even with no items specified,
        # since lazy loading is now permitted.
        return tx

    versioned_transact_write_items(
        builder, dict(table1=[], table2=[]),
    )

    assert ran


def test_lazy_loading_reads_and_writes(
    integration_test_id_table, integration_test_id_table_put, integration_test_id_table_cleaner
):
    tname = integration_test_id_table.name
    test_id_source = "versioned-transact-known-item-read"
    test_id_lazy = "versioned-transact-lazy-load-read"
    test_dest_id = "versioned-transact-write-using-lazy-loaded-value"

    integration_test_id_table_put(dict(id=test_id_source, val=10))
    integration_test_id_table_put(dict(id=test_id_lazy, val=9))
    integration_test_id_table_cleaner(dict(id=test_dest_id))

    def lazy_op(tx: VersionedTransaction) -> VersionedTransaction:
        src = require(tx, tname, dict(id=test_id_source))
        if src["val"] > 5:
            # the if statement here is just an example of why you might want to lazy-load something.
            # In our test, this statement always passes because of the fixture data.
            lazy = require(tx, tname, dict(id=test_id_lazy))
            print(lazy)
            dest_item = dict(id=test_dest_id, val=src["val"] + lazy["val"])
            print(dest_item)
            return put(tx, tname, dest_item)
        # this part of the test is just an example of what you might otherwise do.
        # it's not actually ever going to run in our test.
        return tx

    # note that we only specify upfront a key for the single item we know we need to prefetch
    result = versioned_transact_write_items(lazy_op, {tname: [dict(id=test_id_source)]},)

    assert require(result, tname, dict(id=test_dest_id)) == dict(id=test_dest_id, val=19)


def test_optimistic_delete_nonexistent(integration_test_id_table):
    test_id_to_delete = "versioned-transact-opt-delete"

    def opt_delete(tx: VersionedTransaction) -> VersionedTransaction:
        return delete(tx, integration_test_id_table.name, dict(id=test_id_to_delete))

    res = versioned_transact_write_items(opt_delete, dict())

    assert None is get(res, integration_test_id_table.name, dict(id=test_id_to_delete))


def test_optimistic_delete_existing(integration_test_id_table_put, integration_test_id_table):
    test_id_to_delete = "versioned-transact-opt-delete-existing"

    integration_test_id_table_put(dict(id=test_id_to_delete, val=1984, item_version=4))

    tx_run_count = 0

    def opt_delete(tx: VersionedTransaction) -> VersionedTransaction:
        nonlocal tx_run_count
        tx_run_count += 1
        return delete(tx, integration_test_id_table.name, dict(id=test_id_to_delete))

    res = versioned_transact_write_items(opt_delete, dict())

    assert None is get(res, integration_test_id_table.name, dict(id=test_id_to_delete))

    assert tx_run_count == 2
    # once for the optimistic attempt, which will fail, and a second
    # time for the one that succeeds once it knows what the actual
    # value is.


def test_assert_unchanged(integration_test_id_table_put, integration_test_id_table):
    test_id_to_put = "versioned-transact-put-from-unchanged"
    test_id_to_assert = "versioned-transact-assert-unchanged"

    integration_test_id_table_put(dict(id=test_id_to_assert, val=9))

    def put_after_get(tx):
        a = require(tx, integration_test_id_table.name, dict(id=test_id_to_assert))
        return put(tx, integration_test_id_table.name, dict(id=test_id_to_put, val=a["val"]))

    res = versioned_transact_write_items(
        put_after_get,
        {integration_test_id_table.name: [dict(id=test_id_to_put), dict(id=test_id_to_assert)]},
    )

    assert 9 == require(res, integration_test_id_table.name, dict(id=test_id_to_put))["val"]


def test_dont_write_single_unchanged(integration_test_id_table_put, integration_test_id_table):
    test_key = dict(id="versioned-transact-assert-item-version-unchanged")

    integration_test_id_table_put(dict(test_key, val=4, item_version=3))

    def put_after_get(tx):
        a = require(tx, integration_test_id_table.name, test_key)
        return put(tx, integration_test_id_table.name, a)

    res = versioned_transact_write_items(put_after_get)

    assert require(res, integration_test_id_table.name, test_key)["item_version"] == 3
