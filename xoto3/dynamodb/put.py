"""Utilities for puts and batch puts.

Note that all DynamoDB writer wrappers use a pre-write transform to
try to prevent you from having to do tedious transformations on your
items to make them acceptable to boto3/DynamoDB.

If you need/wish to customize this behavior, look at .prewrite.
"""
from logging import getLogger
from typing import Optional, Tuple, Union

from xoto3.errors import catch_named_clienterrors

from .conditions import item_not_exists
from .constants import DEFAULT_ITEM_NAME
from .exceptions import ItemAlreadyExistsException, get_item_exception_type
from .get import strongly_consistent_get_item
from .prewrite import dynamodb_prewrite
from .types import InputItem, Item, TableResource
from .utils.table import extract_key_from_item, table_primary_keys

logger = getLogger(__name__)


def PutItem(
    Table: TableResource, Item: InputItem, *, nicename=DEFAULT_ITEM_NAME, **kwargs
) -> InputItem:
    """Convenience wrapper that makes your item Dynamo-safe before writing."""
    nicename = nicename or DEFAULT_ITEM_NAME
    logger.debug(f"Put{nicename} into table {Table.name}", extra=dict(json=dict(item=Item)))
    Table.put_item(Item=dynamodb_prewrite(Item), **kwargs)
    return Item


def make_put_item(nicename: str, Table: TableResource):
    def put_item_to_table(Item: InputItem, **kwargs) -> InputItem:
        return PutItem(Table, Item, nicename=nicename, **kwargs)

    return put_item_to_table


def put_unless_exists(Table: TableResource, item: InputItem) -> Tuple[Optional[Exception], dict]:
    """Put item unless it already exists, catching the already exists error and returning it"""
    key_attr_not_exists = item_not_exists(Table.key_schema)
    _put_catch_already_exists = catch_named_clienterrors(
        func=Table.put_item, names=["ConditionalCheckFailedException"]
    )
    already_exists_cerror, response = _put_catch_already_exists(
        **key_attr_not_exists(dict(Item=dynamodb_prewrite(item), ReturnValues="ALL_OLD"))
    )
    if not already_exists_cerror:
        return None, response
    return already_exists_cerror, response


def put_but_raise_if_exists(
    Table: TableResource, item: InputItem, *, nicename: str = DEFAULT_ITEM_NAME,
) -> InputItem:
    """Wrapper for put_item that raises ItemAlreadyExistsException if the item exists,
    or a custom-generated subclass thereof if you have provided a better "nicename".

    If successful, just returns the passed item.

    """
    nicename = nicename or DEFAULT_ITEM_NAME
    already_exists_cerror, _response = put_unless_exists(Table, item)
    if already_exists_cerror:
        raise get_item_exception_type(nicename, ItemAlreadyExistsException)(
            f"{nicename} already exists and was not overwritten!",
            key=extract_key_from_item(Table, item),
            table_name=Table.name,
        )
    return item


def put_or_return_existing(
    table: TableResource, item: InputItem, *, nicename: str = DEFAULT_ITEM_NAME,
) -> Union[Item, InputItem]:
    try:
        put_but_raise_if_exists(table, item, nicename=nicename)
        return item
    except ItemAlreadyExistsException:
        return strongly_consistent_get_item(
            table, {key: item[key] for key in table_primary_keys(table)}, nicename=nicename,
        )
