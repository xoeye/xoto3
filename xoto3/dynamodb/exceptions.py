"""Exceptions for our Dynamo usage"""
from typing import Dict, Optional, Tuple, Type, TypeVar

import botocore.exceptions

from .types import ItemKey


class DynamoDbException(Exception):
    """Wrapping error responses from Dynamo DB"""


class DynamoDbItemException(DynamoDbException):
    def __init__(self, msg: str, *, key: Optional[ItemKey] = None, table_name: str = "", **kwargs):
        self.__dict__.update(kwargs)
        self.key = key
        self.table_name = table_name
        super().__init__(msg)


class AlreadyExistsException(DynamoDbItemException):
    """Deprecated - prefer ItemAlreadyExistsException"""


class ItemAlreadyExistsException(AlreadyExistsException):
    """Backwards-compatible, more consistent name"""


class ItemNotFoundException(DynamoDbItemException):
    """Being more specific that an item was not found"""


X = TypeVar("X", bound=DynamoDbItemException)


_GENERATED_ITEM_EXCEPTION_TYPES: Dict[Tuple[str, str], type] = {
    ("Item", "ItemNotFoundException"): ItemNotFoundException
}


def get_item_exception_type(item_name: str, base_exc: Type[X]) -> Type[X]:
    if not item_name:
        return base_exc
    base_name = base_exc.__name__
    exc_key = (item_name, base_exc.__name__)
    if exc_key not in _GENERATED_ITEM_EXCEPTION_TYPES:
        exc_minus_Item = base_name[4:] if base_name.startswith("Item") else base_name
        _GENERATED_ITEM_EXCEPTION_TYPES[exc_key] = type(
            f"{item_name}{exc_minus_Item}", (base_exc,), dict()
        )
    return _GENERATED_ITEM_EXCEPTION_TYPES[exc_key]


def raise_if_empty_getitem_response(
    getitem_response: dict, nicename="Item", key=None, table_name: str = ""
):
    """Boto3 does not raise any error if the item could not be found. This
    is not what we want in many cases, and it's convenient to have a
    standard way of identifying ItemNotFound.
    """
    if "Item" not in getitem_response:
        key_value = next(iter(key.values())) if key and len(key) == 1 else key
        raise get_item_exception_type(nicename, ItemNotFoundException)(
            f"{nicename} '{key_value}' does not exist!", key=key, table_name=table_name
        )


def translate_clienterrors(client_error: botocore.exceptions.ClientError, names_to_messages: dict):
    """Utility for turning a set of client errors into a matching set of
    more helpful error messages."""
    error_name = client_error.response.get("Error", {}).get("Code")
    if error_name in names_to_messages:
        raise DynamoDbException(names_to_messages[error_name])
    raise client_error  # we don't have a translation for this one
