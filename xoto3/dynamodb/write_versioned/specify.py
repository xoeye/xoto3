"""Low level API for specifying non-effect facts about the state of the database"""
from typing import Optional

from .ddb_api import table_name as _table_name
from .keys import hashable_key, standard_key_attributes
from .types import Item, ItemKey, TableNameOrResource, VersionedTransaction, _TableData


def presume(
    transaction: VersionedTransaction,
    table: TableNameOrResource,
    item_key: ItemKey,
    item_value: Optional[Item],
) -> VersionedTransaction:

    """'To assume as true in the absence of proof to the contrary.'

    Returns a modified transaction with this value set if the value of
    the item is not already known. If a value has already been fetched
    or presumed, this will be a no-op.

    If modified, the presumed value will be available via `get`, and
    will additionally check your presumed value against the table when
    the transaction is run.

    This is purely a cost optimization to avoid fetching something
    when we believe we already know its value. Any transaction builder
    will result in exactly the same data written to the table with or
    without this statement. If the item turns out to have a different
    value in the table than you presumed when the transaction is
    executed, the transaction will restart, the item will be freshly
    fetched like usual, and this procedure will have no effect on the
    transaction.

    This may also be used for the purpose of stubbing out values in an
    empty VersionedTransaction for writing unit tests against your
    transaction builder functions.

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
