from typing import Callable, cast

from xoto3.dynamodb.write_versioned.keys import hashable_key
from xoto3.dynamodb.write_versioned.types import (
    BatchGetItem,
    Item,
    ItemKey,
    ItemKeysByTableName,
    ItemsByTableName,
    Optional,
    VersionedTransaction,
    _TableData,
)


def _probe_table(table_data: _TableData, key: ItemKey) -> Optional[Item]:
    hkey = hashable_key(key)
    if hkey in table_data.effects:
        return table_data.effects[hkey]
    return table_data.items.get(hkey, None)


def transaction_to_batch_getter(vt: VersionedTransaction) -> BatchGetItem:
    """Take a generated transaction and turn it back into an input for a faked transaction run."""

    def batch_get_item(item_keys: ItemKeysByTableName, **_kwargs) -> ItemsByTableName:
        return {
            table_name: list(
                filter(
                    None,
                    [_probe_table(vt.tables[table_name], key) for key in item_keys[table_name]],
                )
            )
            if table_name in vt.tables
            else list()
            for table_name in item_keys
        }

    return cast(BatchGetItem, batch_get_item)


def mock_next_run(
    vt: VersionedTransaction,
    transact_write_items: Callable[[VersionedTransaction], None] = lambda **_kw: None,
):
    return dict(
        batch_get_item=transaction_to_batch_getter(vt), transact_write_items=transact_write_items
    )
