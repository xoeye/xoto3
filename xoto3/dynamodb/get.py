from functools import wraps
from logging import getLogger
from typing import Callable, TypeVar, cast

from xoto3.utils.contextual_default import ContextualDefault

from .constants import DEFAULT_ITEM_NAME
from .exceptions import ItemNotFoundException, raise_if_empty_getitem_response
from .types import Item, ItemKey, TableResource

logger = getLogger(__name__)


GetItem_kwargs: ContextualDefault[dict] = ContextualDefault("get_item_kwargs", dict(), "xoto3-")


@GetItem_kwargs.apply
def GetItem(
    Table: TableResource, Key: ItemKey, nicename=DEFAULT_ITEM_NAME, **get_item_kwargs,
) -> Item:
    """Use this instead of get_item to raise
    {nicename/Item}NotFoundException when an item is not found.

    ```
    with GetItem_kwargs.set_default(dict(ConsistentRead=True)):
        function_that_calls_GetItem(...)
    ```

    to set the default for ConsistentRead differently in different
    contexts without drilling parameters all the way down here. Note
    that an explicitly-provided parameter will always override the
    default.
    """
    nicename = nicename or DEFAULT_ITEM_NAME  # don't allow empty string
    logger.debug(f"Get{nicename} {Key} from Table {Table.name}")
    response = Table.get_item(Key={**Key}, **get_item_kwargs)
    raise_if_empty_getitem_response(response, nicename=nicename, key=Key, table_name=Table.name)
    return response["Item"]


def strongly_consistent_get_item(
    table: TableResource, key: ItemKey, *, nicename: str = DEFAULT_ITEM_NAME,
) -> Item:
    """Shares ItemNotFoundException-raising behavior with GetItem.

    Strongly consistent reads are important when performing
    transactional updates - if you read a stale copy you will be
    likely to fail a transaction retry.
    """
    return GetItem(table, key, ConsistentRead=True, nicename=nicename)


def strongly_consistent_get_item_if_exists(
    table: TableResource, key: ItemKey, *, nicename: str = DEFAULT_ITEM_NAME
) -> Item:
    """Use this for item_getter if you want to do a transactional 'create or update if exists'"""
    try:
        return strongly_consistent_get_item(table, key, nicename=nicename)
    except ItemNotFoundException:
        return dict()


F = TypeVar("F", bound=Callable)


def retry_notfound_consistent_read(get_item: F) -> F:
    @wraps(get_item)
    def get_with_consistent_read_retry(*args, **kwargs):
        try:
            return get_item(*args, **kwargs)
        except ItemNotFoundException:
            con_read = kwargs.get("ConsistentRead")
            if con_read:
                # we already did a consistent read
                raise
            logger.info("Retrying with a consistent read")
            return get_item(*args, **dict(kwargs, ConsistentRead=True))

    return cast(F, get_with_consistent_read_retry)
