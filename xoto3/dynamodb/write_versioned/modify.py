"""Defines the API for modifying an existing transaction. """

from typing import NamedTuple, Optional, Union

from xoto3.dynamodb.constants import DEFAULT_ITEM_NAME
from xoto3.dynamodb.exceptions import get_item_exception_type
from xoto3.dynamodb.prewrite import dynamodb_prewrite
from xoto3.dynamodb.types import Item, ItemKey
from xoto3.utils.tree_map import SimpleTransform

from .ddb_api import table_name as _table_name
from .errors import ItemUnknownToTransactionError, TableUnknownToTransactionError
from .keys import hashable_key, key_from_item
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
    """Shared put/delete implementation - not meant for direct use at this time."""
    table_name = _table_name(table)
    if table_name not in transaction.tables:
        raise TableUnknownToTransactionError(table_name)
    items, effects, key_attributes = transaction.tables[table_name]

    item_or_none, item_key = (
        (put_or_delete.item, key_from_item(key_attributes, put_or_delete.item))
        if isinstance(put_or_delete, Put)
        else (None, key_from_item(key_attributes, put_or_delete.item_key))
    )

    hashable_item_key = hashable_key(item_key)
    if hashable_item_key not in items:
        # Any item to be modified by the transaction must be specified
        # at the initiation of the transaction so that it can be
        # prefetched.
        raise get_item_exception_type(nicename, ItemUnknownToTransactionError)(
            "Not specified as part of transaction", key=item_key, table_name=table_name
        )

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
