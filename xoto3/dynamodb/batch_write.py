from typing import Optional, Iterable, Any
import timeit
from logging import getLogger
from typing_extensions import TypedDict

from xoto3.backoff import backoff
from xoto3.utils.iter import grouper_it

from .types import InputItem, Item, ItemKey, TableResource
from .prewrite import dynamodb_prewrite


logger = getLogger(__name__)


class PutOrDelete(TypedDict):
    put_item: Optional[InputItem]
    delete_key: Optional[ItemKey]


def BatchPut(
    table: TableResource,
    items: Iterable[Item],
    *,
    error_on_duplicates: bool = False,  # based on primary key of items
):
    BatchWriteItem(
        table,
        (dict(put_item=item, delete_key=None) for item in items),
        error_on_duplicates=error_on_duplicates,
    )


def BatchDelete(table: TableResource, item_keys: Iterable[Any]):
    BatchWriteItem(table, (dict(delete_key=key, put_item=None) for key in item_keys))


def BatchWriteItem(
    table: TableResource,
    actions: Iterable[PutOrDelete],
    *,
    error_on_duplicates: bool = False,  # based on primary key of items
):
    """At least once processing"""
    start = timeit.default_timer()
    num_written = 0

    primary_keys = (
        [key["AttributeName"] for key in table.key_schema] if not error_on_duplicates else None
    )

    with table.batch_writer(overwrite_by_pkeys=primary_keys) as batch_writer:
        put_item_with_backoff = backoff(batch_writer.put_item)
        delete_item_with_backoff = backoff(batch_writer.delete_item)
        for iter_50 in grouper_it(50, actions):
            try:
                batch_50 = list(iter_50)
                for action in batch_50:
                    put_item = action.get("put_item")
                    delete_key = action.get("delete_key")
                    if put_item:
                        put_item_with_backoff(Item=dynamodb_prewrite(put_item))
                    elif delete_key:
                        delete_item_with_backoff(Key=delete_key)
                    else:
                        logger.warning("Provided empty action - ignoring")
                    num_written += 1
                    if num_written % 1000 == 0:
                        logger.info(
                            f"Large partial write report; have written {num_written} "
                            f"items to {table.name} in this batch"
                        )
            except Exception as e:
                logger.exception(e)
                logger.error(
                    f"Failed to perform BatchWrite to table {table.name} with batch {batch_50}",
                    extra=dict(json=dict(batch_50=batch_50)),
                )
                raise e
            if num_written:
                ms_elapsed = (timeit.default_timer() - start) * 1000
                logger.debug(
                    f"BatchWrite to {table.name} wrote {num_written} items in "
                    f"{int(ms_elapsed)} ms; {num_written/ms_elapsed*1000:.02f}/s"
                )
