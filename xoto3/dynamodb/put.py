"""Utilities for puts and batch puts.

Note that all DynamoDB writer wrappers use a pre-write transform to
try to prevent you from having to do tedious transformations on your
items to make them acceptable to boto3/DynamoDB.

If you need/wish to customize this behavior, look at .prewrite.
"""
from typing import Tuple, Optional
from logging import getLogger

from xoto3.errors import catch_named_clienterrors

from .conditions import item_not_exists
from .exceptions import AlreadyExistsException
from .types import InputItem, Item, TableResource
from .prewrite import dynamodb_prewrite


logger = getLogger(__name__)


def PutItem(Table: TableResource, Item: InputItem, *, nicename="Item", **kwargs) -> InputItem:
    """Convenience wrapper that makes your item Dynamo-safe before writing.
    """
    logger.debug(f"Put{nicename} into table {Table.name}", extra=dict(json=dict(item=Item)))
    Table.put_item(Item=dynamodb_prewrite(Item), **kwargs)
    return Item


def make_put_item(nicename: str, Table: TableResource):
    def put_item_to_table(Item: InputItem, **kwargs) -> InputItem:
        return PutItem(Table, Item, nicename=nicename, **kwargs)

    return put_item_to_table


def put_unless_exists(Table: TableResource, item: InputItem) -> Tuple[Optional[Exception], Item]:
    """Put item unless it already exists, catching the already exists error and returning it"""
    _put_catch_already_exists = catch_named_clienterrors(
        func=Table.put_item, names=["ConditionalCheckFailedException"]
    )
    already_exists_cerror, response = _put_catch_already_exists(
        Item=dynamodb_prewrite(item),
        ConditionExpression=item_not_exists(Table.key_schema),
        ReturnValues="ALL_OLD",
    )
    return already_exists_cerror, response["Item"]


def put_but_raise_if_exists(
    Table: TableResource, item: InputItem, *, nicename: str = "item"
) -> InputItem:
    """Wrapper for put_item that raises a standard exception if the item exists.

    Just returns the passed item."""
    already_exists_cerror, _response = put_unless_exists(Table, item)
    if already_exists_cerror:
        raise AlreadyExistsException(f"{nicename} already exists and was not overwritten!")
    return item
