"""Low level API for specifying non-effect facts about the state of the database"""
from typing import Optional

from xoto3.dynamodb.prewrite import dynamodb_prewrite
from xoto3.utils.tree_map import SimpleTransform

from .ddb_api import table_name as _table_name
from .keys import hashable_key, standard_key_attributes
from .types import Item, ItemKey, TableNameOrResource, VersionedTransaction, _TableData


def presume(
    transaction: VersionedTransaction,
    table: TableNameOrResource,
    item_key: ItemKey,
    item_value: Optional[Item],
    *,
    prewrite_transform: Optional[SimpleTransform] = None,
) -> VersionedTransaction:

    """'To assume as true in the absence of proof to the contrary.'

    Returns a modified transaction with this value set if the value of
    the item is not already known. If a value has already been fetched
    or presumed, this will be a no-op.

    If modified, the presumed value will be available via `get`, and
    will additionally check your presumed value against the table when
    the transaction is run.

    At runtime (within the context of
    `versioned_transact_write_items`), this is purely a cost
    optimization to avoid fetching an item for which you believe you
    already know the value. Whether your presumption is right, wrong,
    or you don't presume anything, your transaction builder will
    result in exactly the same data written to the table.

    As with any item fetched or presumed, if the item turns out to
    have a different value in the table than you presumed when the
    transaction is committed, the transaction will restart, the item
    will be freshly fetched like usual, and your use of presumption
    will have no ultimate effect on the data.

    For unit testing, this is the approved approach for setting up a
    VersionedTransaction 'fixture', where you declare the state of the
    database before your transaction builder is run. Set a presumed
    value for every item that you will attempt to `get` or `require`
    within your transaction builder, otherwise your test will error
    with ItemUndefinedException.

    """
    if item_value is not None:
        for key_attr, key_val in item_key.items():
            assert item_value[key_attr] == key_val, "Item key must match in a non-nil item value"
    table_name = _table_name(table)
    if table_name in transaction.tables:
        table_data = transaction.tables[table_name]
    else:
        table_data = _TableData(
            items=dict(), effects=dict(), key_attributes=standard_key_attributes(*item_key.keys())
        )
    hkey = hashable_key(item_key)
    if hkey not in table_data.items:
        item_value = dynamodb_prewrite(item_value, prewrite_transform) if item_value else None
        # this prewrite makes sure the value looks like it could have come out of DynamoDB.
        return VersionedTransaction(
            tables={
                **transaction.tables,
                table_name: _TableData(
                    items={**table_data.items, hkey: item_value},
                    effects=table_data.effects,
                    key_attributes=table_data.key_attributes,
                ),
            }
        )
    return transaction


def define_table(
    transaction: VersionedTransaction, table: TableNameOrResource, *key_attributes: str,
) -> VersionedTransaction:
    """Idempotent definition of key attribute schema for the given table
    without forcing any IO operations/effects up front.

    The main reason you might want to do this is if you need to do a
    `put`, because `put` cannot infer the shape of your key.

    If the table definition is already present, this is a no-op.

    """
    assert len(key_attributes) > 0 and len(key_attributes) <= 2
    if _table_name(table) in transaction.tables:
        return transaction
    return VersionedTransaction(
        tables={
            **transaction.tables,
            _table_name(table): _TableData(
                items=dict(),
                effects=dict(),
                key_attributes=standard_key_attributes(*key_attributes),
            ),
        }
    )
