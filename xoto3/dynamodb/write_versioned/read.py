"""API for reading from a Versioned Transaction"""

from copy import deepcopy
from typing import Callable, Optional, cast

from xoto3.dynamodb.constants import DEFAULT_ITEM_NAME
from xoto3.dynamodb.exceptions import get_item_exception_type, raise_if_empty_getitem_response
from xoto3.dynamodb.types import Item, ItemKey

from .ddb_api import table_name as _table_name
from .errors import ItemNotYetFetchedError
from .keys import hashable_key
from .types import TableNameOrResource, VersionedTransaction


def _ident(i: Item) -> Item:
    return i


def get(
    transaction: VersionedTransaction,
    table: TableNameOrResource,
    item_key: ItemKey,
    *,
    copy: bool = True,
    nicename: str = DEFAULT_ITEM_NAME,
) -> Optional[Item]:
    """Returns current state of the transactable item according to the
    transaction.
    This is Python, so the only way we can stop you from modifying
    this canonical reference for the current value of the item is to
    return a deep copy. That's expensive, so if you trust yourself you
    can disable that default behavior.
    HOWEVER, the behavior of this system is **undefined** if you
    disable this behavior and then modify one of the retrieved items
    directly. Caveat emptor...
    """
    table_name = _table_name(table)
    item_exc_type = get_item_exception_type(nicename, ItemNotYetFetchedError)
    if table_name not in transaction.tables:
        raise item_exc_type("Table new to transaction", key=item_key, table_name=table_name)

    items, effects, _ = transaction.tables[table_name]
    item_hashable_key = hashable_key(item_key)

    xf_result = cast(Callable[[Optional[Item]], Optional[Item]], deepcopy if copy else _ident)
    if item_hashable_key in effects:
        return xf_result(effects[item_hashable_key])
    if item_hashable_key not in items:
        raise item_exc_type(
            f"{nicename} not yet present in transaction", key=item_key, table_name=table_name
        )

    return xf_result(items[item_hashable_key])


def require(
    transaction: VersionedTransaction,
    table: TableNameOrResource,
    item_key: ItemKey,
    *,
    nicename: str = DEFAULT_ITEM_NAME,
    **kwargs,
) -> Item:
    table_name = _table_name(table)
    item = get(transaction, table_name, item_key, nicename=nicename, **kwargs)
    if not item:
        raise_if_empty_getitem_response(
            dict(), nicename=nicename, key=item_key, table_name=table_name
        )
    assert item is not None
    return item
