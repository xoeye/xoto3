"""Defines the API for modifying an existing transaction. """

from typing import Mapping, NamedTuple, Optional, Union

from xoto3.dynamodb.constants import DEFAULT_ITEM_NAME
from xoto3.dynamodb.prewrite import dynamodb_prewrite
from xoto3.dynamodb.types import Item, ItemKey
from xoto3.utils.tree_map import SimpleTransform

from .ddb_api import known_key_schema
from .ddb_api import table_name as _table_name
from .errors import TableSchemaUnknownError
from .keys import hashable_key, key_from_item
from .prepare import standard_key_attributes_from_key
from .types import HashableItemKey, TableNameOrResource, VersionedTransaction, _TableData


class Put(NamedTuple):
    item: Item


class Delete(NamedTuple):
    item_key: ItemKey


PutOrDelete = Union[Put, Delete]


def _drop_noop_puts(
    items: Mapping[HashableItemKey, Optional[Item]],
    effects: Mapping[HashableItemKey, Union[Item, None]],
    new_puts: Mapping[HashableItemKey, Item],
) -> Mapping[HashableItemKey, Union[Item, None]]:
    current = {**items, **effects}
    return {
        item_key: item_value
        for item_key, item_value in new_puts.items()
        if item_value != current.get(item_key)
    }


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
                        "and it is not guessable from the delete you requested. "
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

    effect = (
        _drop_noop_puts(
            items,
            effects,
            {hashable_key(key_from_item(key_attributes, put_or_delete.item)): put_or_delete.item},
        )
        if isinstance(put_or_delete, Put)
        else {hashable_key(key_from_item(key_attributes, put_or_delete.item_key)): None}
    )
    if not effect:
        return transaction

    return VersionedTransaction(
        tables={
            **transaction.tables,
            table_name: _TableData(
                items=items, effects={**effects, **effect}, key_attributes=key_attributes,
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
