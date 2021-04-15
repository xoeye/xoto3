"""Defines the API for modifying an existing transaction. """

from typing import NamedTuple, Optional, Union

from xoto3.dynamodb.constants import DEFAULT_ITEM_NAME
from xoto3.dynamodb.prewrite import dynamodb_prewrite
from xoto3.dynamodb.types import Item, ItemKey
from xoto3.utils.tree_map import SimpleTransform

from .ddb_api import known_key_schema
from .ddb_api import table_name as _table_name
from .errors import TableSchemaUnknownError
from .keys import hashable_key, key_from_item
from .prepare import standard_key_attributes_from_key
from .types import TableNameOrResource, VersionedTransaction, _TableData


class Put(NamedTuple):
    item: Item


class Delete(NamedTuple):
    item_key: ItemKey


PutOrDelete = Union[Put, Delete]


def _write(
    transaction: VersionedTransaction,
    table: TableNameOrResource,
    put_or_delete: PutOrDelete,
    *,
    nicename: str = DEFAULT_ITEM_NAME,
) -> VersionedTransaction:
    """Shared put/delete implementation - not meant for direct use at this time.

    Performs an optimistic put - if the item is not known to the
    existing transaction, assumes you mean to create it if and only if
    it does not exist. If the item turns out to exist already, your
    transaction will be re-run, at which point a put will be interpreted as a
    'witting' choice to overwrite the known item.
    """

    table_name = _table_name(table)

    if table_name in transaction.tables:
        items, effects, key_attributes = transaction.tables[table_name]
    else:
        # we have to have key_attributes in order to proceed with any
        # effect for the given table. We're going to attempt various
        # ways of getting them, starting by asking DynamoDB directly.
        key_attributes = known_key_schema(table)
        items = dict()
        effects = dict()
        if not key_attributes:
            if isinstance(put_or_delete, Delete):
                # hope that the user provided an actual item key
                if len(put_or_delete.item_key) > 2:
                    raise TableSchemaUnknownError(
                        f"We don't know the key schema for {table_name} because you haven't defined it "
                        "and it is not guessable from the delete you requested."
                        "Specify this delete in terms of the key only and this should work fine."
                    )
                # at this point this is a best guess
                key_attributes = standard_key_attributes_from_key(put_or_delete.item_key)
            else:
                # it's a put - we can't make this work at all
                raise TableSchemaUnknownError(
                    f"We don't have enough information about table {table_name} to properly derive "
                    "a key from your request to put this item. "
                    "Prefetching this item by key would solve that problem."
                )

    item_or_none, item_key = (
        (put_or_delete.item, key_from_item(key_attributes, put_or_delete.item))
        if isinstance(put_or_delete, Put)
        else (None, key_from_item(key_attributes, put_or_delete.item_key))
    )

    hashable_item_key = hashable_key(item_key)

    return VersionedTransaction(
        tables={
            **transaction.tables,
            table_name: _TableData(
                items=items,
                effects={**effects, hashable_item_key: item_or_none},
                key_attributes=key_attributes,
            ),
        }
    )


def put(
    transaction: VersionedTransaction,
    table: TableNameOrResource,
    item: Item,
    *,
    nicename: str = DEFAULT_ITEM_NAME,
    prewrite_transform: Optional[SimpleTransform] = None,
) -> VersionedTransaction:
    """Returns a modified transaction including the requested PutItem operation"""
    return _write(
        transaction, table, Put(dynamodb_prewrite(item, prewrite_transform)), nicename=nicename
    )


def delete(
    transaction: VersionedTransaction,
    table: TableNameOrResource,
    item_or_key: Union[Item, ItemKey],
    *,
    nicename: str = DEFAULT_ITEM_NAME,
) -> VersionedTransaction:
    """Returns a modified transaction including the requested DeleteItem operation"""
    return _write(transaction, table, Delete(item_or_key), nicename=nicename)


def presume(
    transaction: VersionedTransaction,
    table: TableNameOrResource,
    item_key: ItemKey,
    item_value: Optional[Item],
) -> VersionedTransaction:

    """'To assume as true in the absence of proof to the contrary.'

    Returns a modified transaction if the value of the item is not already
    known. This will make the presumed value available via `get`,
    and will additionally check your presumed value against the table when
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
            items=dict(), effects=dict(), key_attributes=standard_key_attributes_from_key(item_key)
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
    """
    This is a convenient way of dynamically defining a key
    attribute schema for a given table without forcing any IO
    operations/effects up front.
    """
    assert len(key_attributes) > 0 and len(key_attributes) <= 2
    if _table_name(table) in transaction.tables:
        return transaction
    return VersionedTransaction(
        tables={
            **transaction.tables,
            _table_name(table): _TableData(
                items=dict(), effects=dict(), key_attributes=tuple(sorted(key_attributes)),
            ),
        }
    )
