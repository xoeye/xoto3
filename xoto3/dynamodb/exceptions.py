"""Exceptions for our Dynamo usage"""
import botocore.exceptions


class DynamoDbException(Exception):
    """Wrapping error responses from Dynamo DB"""

    pass


class AlreadyExistsException(DynamoDbException):
    pass


class ItemNotFoundException(DynamoDbException):
    """Being more specific that an item was not found"""


def raise_if_empty_getitem_response(getitem_response: dict, nicename="Item", key=None):
    """Boto3 does not raise any error if the item could not be found"""
    if "Item" not in getitem_response:
        if "id" in key and len(key) == 1:
            key = key["id"]
        raise ItemNotFoundException(f"{nicename} '{key}' does not exist!")


def translate_clienterrors(client_error: botocore.exceptions.ClientError, names_to_messages: dict):
    """Utility for turning a set of client errors into a matching set of
    more helpful error messages."""
    error_name = client_error.response.get("Error", {}).get("Code")
    if error_name in names_to_messages:
        raise DynamoDbException(names_to_messages[error_name])
    raise client_error  # we don't have a translation for this one
