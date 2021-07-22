"""Private implementation details for versioned_transact_write_items"""
from collections import defaultdict
from functools import partial
from logging import getLogger
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, cast

from botocore.exceptions import ClientError
from typing_extensions import Protocol, TypedDict

from xoto3.dynamodb.types import Item
from xoto3.dynamodb.update.retry import is_conditional_update_retryable
from xoto3.dynamodb.utils.expressions import versioned_item_expression
from xoto3.dynamodb.utils.serde import serialize_item
from xoto3.dynamodb.utils.table import table_primary_keys
from xoto3.errors import client_error_name
from xoto3.lazy_session import tll_from_session

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


_DDB_RES = tll_from_session(lambda s: s.resource("dynamodb"))
_DDB_CLIENT = tll_from_session(lambda s: s.client("dynamodb"))


def table_name(table: TableNameOrResource) -> str:
    if not isinstance(table, str):
        return table.name
    return table


def known_key_schema(table: TableNameOrResource) -> Tuple[str, ...]:
    """Note that this code may perform I/O, and may run during the
    operation of your transaction builder.

    Technically that is a side effect, and also this requires
    permissions to read the key schema of your table using
    DynamoDB::DescribeTable.

    You can avoid ever having this happen just by making sure to
    define the key schema of your table prior to performing a put or
    delete, either via a get/require, or via the define_table
    mechanism. In the vast majority of cases, you should not find
    yourself in this situation, but if you do get an exception
    bubbling up through here, that's what's going on, and you should
    probably add a call to `define_table` at the beginning of your
    transaction.
    """
    try:
        if isinstance(table, str):
            table = _DDB_RES().Table(table)
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


_RetryableTransactionCancelledErrorCodes = {
    "ConditionalCheckFailed",
    "TransactionConflict",
    "ThrottlingError",
    "ProvisionedThroughputExceeded",
}


def _is_transaction_failed_and_retryable(ce: ClientError) -> bool:
    error_name = client_error_name(ce)
    if error_name == "TransactionInProgressException":
        return True
    if error_name == "TransactionCanceledException":
        return _collect_codes(ce.response) <= _RetryableTransactionCancelledErrorCodes
    return False


def is_cancelled_and_retryable(ce: ClientError) -> bool:
    return is_conditional_update_retryable(ce) or _is_transaction_failed_and_retryable(ce)


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
    results: Dict[str, List[Item]] = defaultdict(list)
    while unprocessed_keys:
        _log.debug(f"Performing batch_get of {len(unprocessed_keys)} keys")
        response = batch_get_item(RequestItems=unprocessed_keys)
        unprocessed_keys = response.get("UnprocessedKeys")  # type: ignore
        for table_name, items in response["Responses"].items():
            results[table_name].extend(items)
    return results


def make_transact_multiple_but_optimize_single(ddb_client):
    def boto3_transact_multiple_but_optimize_single(TransactItems: List[dict], **kwargs) -> Any:
        if len(TransactItems) == 0:
            _log.debug("Nothing to transact - returning")
            return
        # ClientRequestToken, if provided, indicates a desire to use
        # certain idempotency guarantees provided only by
        # TransactWriteItems. I'm not sure if it's even relevant for a
        # single-item operation, but it's an issue of expense, not
        # correctness, to leave it in.
        if len(TransactItems) == 1 and "ClientRequestToken" not in kwargs:
            # attempt simple condition-checked put or delete to halve the cost
            command = TransactItems[0]
            item_args = tuple(command.values())[0]  # first and only value is a dict of arguments
            if set(command) == {"Put"}:
                ddb_client.put_item(**{**item_args, **kwargs})
                return
            if set(command) == {"Delete"}:
                ddb_client.delete_item(**{**item_args, **kwargs})
                return
            if set(command) == {"ConditionCheck"}:
                _log.debug(
                    "Item was not modified and is solitary - no need to interact with the table"
                )
                return
            # we don't (yet) support single write optimization for things other than Put or Delete
        ddb_client.transact_write_items(TransactItems=TransactItems, **kwargs)

    return boto3_transact_multiple_but_optimize_single


def boto3_impl_defaults(
    batch_get_item: Optional[BatchGetItem] = None,
    transact_write_items: Optional[TransactWriteItems] = None,
) -> Tuple[BatchGetItem, TransactWriteItems]:
    if not batch_get_item:
        batch_get_item = cast(BatchGetItem, partial(_ddb_batch_get_item, _DDB_RES().batch_get_item))
    assert batch_get_item

    if not transact_write_items:
        transact_write_items = make_transact_multiple_but_optimize_single(_DDB_CLIENT())
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
    item_version_attribute: str = "item_version",
    last_written_attribute: str = "last_written_at",
) -> dict:
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
            expression_expecting_item_version = _serialize_versioned_expr(
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
                        **expression_expecting_item_version,
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
                    **expression_expecting_item_version,
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
            """This will also check that the item still does not exist if it previously did not"""
            expression_expecting_item_version = _serialize_versioned_expr(
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
                    **expression_expecting_item_version,
                )
            )

        transact_items.extend(
            [
                item_remains_unmodified(item_hashable_key, item)
                for item_hashable_key, item in items.items()
                if item_hashable_key not in keys_of_items_to_be_modified
            ]
        )

    return dict(TransactItems=transact_items)
