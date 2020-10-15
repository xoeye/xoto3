from logging import getLogger

from .constants import DEFAULT_ITEM_NAME
from .exceptions import ItemNotFoundException, raise_if_empty_getitem_response
from .types import Item, ItemKey, TableResource

logger = getLogger(__name__)


def GetItem(Table: TableResource, Key: ItemKey, nicename=DEFAULT_ITEM_NAME, **kwargs) -> Item:
    """Use this if possible instead of get_item directly

    because the default behavior of the boto3 get_item is bad (doesn't
    fail if no item was found).

    """
    nicename = nicename or DEFAULT_ITEM_NAME  # don't allow empty string
    logger.debug(f"Get{nicename} {Key} from Table {Table.name}")
    response = Table.get_item(Key={**Key}, **kwargs)
    raise_if_empty_getitem_response(response, nicename=nicename, key=Key, table_name=Table.name)
    return response["Item"]


def strongly_consistent_get_item(
    table: TableResource, key: ItemKey, *, nicename: str = DEFAULT_ITEM_NAME,
) -> Item:
    """This is the default getter for a reason.

    GetItem raises if the item does not exist, preventing you from updating
    something that does not exist.

    Strongly consistent reads are important when performing updates - if you
    read a stale copy you will be guaranteed to fail your update.
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
