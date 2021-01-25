"""Private implementation details for versioned_transact_write_items"""
from collections import defaultdict
from functools import partial
from logging import getLogger
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, cast

import boto3
from botocore.exceptions import ClientError
from typing_extensions import Protocol, TypedDict

from xoto3.dynamodb.types import Item
from xoto3.dynamodb.update.versioned import versioned_item_expression
from xoto3.dynamodb.utils.serde import serialize_item
from xoto3.dynamodb.utils.table import table_primary_keys
from xoto3.errors import client_error_name

from .keys import hashable_key_to_key
from .types import (
    BatchGetItem,
    HashableItemKey,
    ItemKeysByTableName,
    ItemsByTableName,
    TableNameOrResource,
    TransactWriteItems,
    VersionedTransaction,
)

_log = getLogger(__name__)

_RetryableTransactionCancelledErrorCodes = {
    "ConditionalCheckFailed",
    "TransactionConflict",
    "ThrottlingError",
    "ProvisionedThroughputExceeded",
}


def table_name(table: TableNameOrResource) -> str:
    if not isinstance(table, str):
        return table.name
    return table


def known_key_schema(table: TableNameOrResource) -> Tuple[str, ...]:
    try:
        if isinstance(table, str):
            table = boto3.resource("dynamodb").Table(table)
        assert not isinstance(table, str)
        # your environment may or may not have permissions to read the key schema of its table.
        # in general, that is a nice permission to allow if possible.
        return table_primary_keys(table)
    except ClientError:
        _log.warning("Key schema could not be fetched")
    return tuple()  # unknown!


def _collect_codes(resp: dict) -> Set[str]:
    cc = {reason["Code"] for reason in resp["Error"].get("CancellationReasons", tuple())}
    return cc


def is_cancelled_and_retryable(ce: ClientError) -> bool:
    return (
        client_error_name(ce) in ("TransactionCanceledException", "ConditionalCheckFailedException")
        and _collect_codes(ce.response) <= _RetryableTransactionCancelledErrorCodes
    )


class BatchGetResponse(TypedDict):
    Responses: Mapping[str, List[Item]]


class Boto3BatchGetItem(Protocol):
    def __call__(self, RequestItems: Mapping[str, dict], **__kwargs) -> BatchGetResponse:
        ...  # pragma: nocover


def _ddb_batch_get_item(
    batch_get_item: Boto3BatchGetItem, item_keys_by_table_name: ItemKeysByTableName,
) -> ItemsByTableName:
    unprocessed_keys = {
        table_name: dict(Keys=item_keys, ConsistentRead=True)
        for table_name, item_keys in item_keys_by_table_name.items()
        if item_keys  # don't make empty request to a table
    }
    results = defaultdict(list)
    while unprocessed_keys:
        response = batch_get_item(RequestItems=unprocessed_keys)
        unprocessed_keys = response.get("UnprocessedKeys")  # type: ignore
        for table_name, items in response["Responses"].items():
            results[table_name].extend(items)
    return results


def make_transact_multiple_but_optimize_single(ddb_client):
    def boto3_transact_multiple_but_optimize_single(TransactItems: List[dict], **kwargs) -> Any:
        if len(TransactItems) == 1 and "ClientRequestToken" not in kwargs:
            # attempt single put or delete to halve the cost
            command = TransactItems[0]
            item_args = tuple(command.values())[0]  # first and only value is a dict of arguments
            if set(command) == {"Put"}:
                ddb_client.put_item(**{**item_args, **kwargs})
                return
            if set(command) == {"Delete"}:
                ddb_client.delete_item(**{**item_args, **kwargs})
                return
            # we don't (yet) support single writee optimization for things other than Put or Delete
        ddb_client.transact_write_items(TransactItems=TransactItems, **kwargs)

    return boto3_transact_multiple_but_optimize_single


def boto3_impl_defaults(
    batch_get_item: Optional[BatchGetItem] = None,
    transact_write_items: Optional[TransactWriteItems] = None,
) -> Tuple[BatchGetItem, TransactWriteItems]:
    resource = None
    if not batch_get_item:
        resource = boto3.resource("dynamodb")
        batch_get_item = cast(BatchGetItem, partial(_ddb_batch_get_item, resource.batch_get_item))
    assert batch_get_item

    if not transact_write_items:
        transact_write_items = make_transact_multiple_but_optimize_single(boto3.client("dynamodb"))
    assert transact_write_items

    return (
        batch_get_item,
        transact_write_items,
    )


def _serialize_versioned_expr(expr: dict) -> dict:
    return dict(expr, ExpressionAttributeValues=serialize_item(expr["ExpressionAttributeValues"]))


def built_transaction_to_transact_write_items_args(
    transaction: VersionedTransaction,
    last_written_at_str: str,
    ClientRequestToken: str = "",
    item_version_attribute: str = "item_version",
    last_written_attribute: str = "last_written_at",
) -> dict:

    args: Dict[str, Any] = dict(ClientRequestToken=ClientRequestToken)
    if not ClientRequestToken:
        args.pop("ClientRequestToken")
    transact_items = list()
    for table_name, tbl_data in transaction.tables.items():
        items, effects, key_attributes = tbl_data

        def get_existing_item(hashable_key) -> dict:
            return items.get(hashable_key) or dict()

        keys_of_items_to_be_modified = set()

        def put_or_delete_item(item_hashable_key: HashableItemKey, effect: Optional[Item]) -> dict:
            keys_of_items_to_be_modified.add(item_hashable_key)
            item = get_existing_item(item_hashable_key)
            expected_version = item.get(item_version_attribute, 0)
            expression_ensuring_unmodifiedness = _serialize_versioned_expr(
                versioned_item_expression(
                    expected_version,
                    item_version_key=item_version_attribute,
                    id_that_exists=key_attributes[0] if item else "",
                )
            )
            if effect is None:
                # item is nil, indicating requested delete
                return dict(
                    Delete=dict(
                        TableName=table_name,
                        Key=serialize_item(
                            dict(hashable_key_to_key(key_attributes, item_hashable_key))
                        ),
                        **expression_ensuring_unmodifiedness,
                    )
                )

            # put
            return dict(
                Put=dict(
                    TableName=table_name,
                    Item=serialize_item(
                        dict(
                            effect,
                            **{
                                last_written_attribute: last_written_at_str,
                                item_version_attribute: expected_version + 1,
                            },
                        )
                    ),
                    **expression_ensuring_unmodifiedness,
                )
            )

        transact_items.extend(
            [
                put_or_delete_item(item_hashable_key, effect)
                for item_hashable_key, effect in effects.items()
            ]
        )

        def item_remains_unmodified(
            item_hashable_key: HashableItemKey, item: Optional[Item]
        ) -> dict:
            expression_ensuring_unmodifiedness = _serialize_versioned_expr(
                versioned_item_expression(
                    get_existing_item(item_hashable_key).get(item_version_attribute, 0),
                    item_version_key=item_version_attribute,
                    id_that_exists=key_attributes[0] if item else "",
                )
            )
            return dict(
                ConditionCheck=dict(
                    TableName=table_name,
                    Key=serialize_item(
                        dict(hashable_key_to_key(key_attributes, item_hashable_key))
                    ),
                    **expression_ensuring_unmodifiedness,
                )
            )

        transact_items.extend(
            [
                item_remains_unmodified(item_hashable_key, item)
                for item_hashable_key, item in items.items()
                if item_hashable_key not in keys_of_items_to_be_modified
            ]
        )

    args["TransactItems"] = transact_items
    return args
