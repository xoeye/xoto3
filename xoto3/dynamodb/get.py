from logging import getLogger

from .types import TableResource, ItemKey, Item
from .exceptions import raise_if_empty_getitem_response, ItemNotFoundException


logger = getLogger(__name__)


def GetItem(Table: TableResource, Key: ItemKey, nicename="Item", **kwargs) -> Item:
    """Use this if possible instead of get_item directly

    because the default behavior of the boto3 get_item is bad (doesn't
    fail if no item was found).

    """
    logger.debug(f"Get{nicename} {Key} from Table {Table.name}")
    response = Table.get_item(Key={**Key}, **kwargs)
    raise_if_empty_getitem_response(response, nicename, key=Key)
    return response["Item"]


def strongly_consistent_get_item(table: TableResource, key: ItemKey) -> Item:
    """This is the default getter for a reason.

    GetItem raises if the item does not exist, preventing you from updating
    something that does not exist.

    Strongly consistent reads are important when performing updates - if you
    read a stale copy you will be guaranteed to fail your update.
    """
    return GetItem(table, key, ConsistentRead=True)


def strongly_consistent_get_item_if_exists(table: TableResource, key: ItemKey) -> Item:
    """Use this for item_getter if you want to do a transactional 'create or update if exists'"""
    try:
        return strongly_consistent_get_item(table, key)
    except ItemNotFoundException:
        return dict()
