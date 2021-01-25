"""The core run loop for a transaction"""
from datetime import datetime
from logging import getLogger
from typing import Callable, Collection, Iterable, Mapping, Optional, Tuple

from botocore.exceptions import ClientError

from xoto3.dynamodb.types import ItemKey
from xoto3.utils.dt import iso8601strict

from .ddb_api import (
    boto3_impl_defaults,
    built_transaction_to_transact_write_items_args,
    is_cancelled_and_retryable,
)
from .errors import ItemNotYetFetchedError, TransactionAttemptsOverrun
from .prepare import (
    add_item_to_base_request,
    all_items_for_next_attempt,
    parse_batch_get_request,
    prepare_clean_transaction,
)
from .retry import timed_retry
from .types import BatchGetItem, TransactionBuilder, TransactWriteItems, VersionedTransaction

logger = getLogger(__name__)


def _is_empty(transaction: VersionedTransaction) -> bool:
    return not sum(len(effects) for _, effects, *_ in transaction.tables.values())


def _lazy_loading_transaction_builder(
    transaction_builder: TransactionBuilder,
    item_keys_by_table_name: Mapping[str, Collection[ItemKey]],
    batch_get_item: BatchGetItem,
) -> Tuple[VersionedTransaction, VersionedTransaction]:
    """If your transaction attempts to get/require an item that was not
    prefetched, we will stop, fetch it, and then retry your
    transaction from the beginning.

    Remember to make your transaction a pure function, or at least make
    sure all of its side effects are effectively idempotent!
    """
    while True:
        item_keys_by_table_name = parse_batch_get_request(item_keys_by_table_name)
        clean_transaction = prepare_clean_transaction(
            item_keys_by_table_name, batch_get_item(item_keys_by_table_name),
        )
        try:
            built_transaction = transaction_builder(clean_transaction)
            # the built transaction will always contain a superset of the keys
            # present in the clean transaction.
            return clean_transaction, built_transaction
        except ItemNotYetFetchedError as item_error:
            assert item_error.key
            item_keys_by_table_name = add_item_to_base_request(
                item_keys_by_table_name, (item_error.table_name, item_error.key),
            )


def versioned_transact_write_items(
    transaction_builder: TransactionBuilder,
    item_keys_by_table_name: Mapping[str, Collection[ItemKey]] = dict(),
    *,
    batch_get_item: Optional[BatchGetItem] = None,
    transact_write_items: Optional[TransactWriteItems] = None,
    is_retryable: Callable[[ClientError], bool] = is_cancelled_and_retryable,
    attempts_iterator: Optional[Iterable] = None,
    item_version_attribute: str = "item_version",
    last_written_attribute: str = "last_written_at",
) -> VersionedTransaction:
    """Performs a read-transact-write loop over multiple items across
    multiple tables until there are no interruptions causing the
    transaction to fail, or until the transaction expires (as
    specified by the attempts_iterator or the default timed_retry).

    Allows PUTs and DELETEs - an UPDATE with versioning is no different
    from a PUT.

    You do not have to write back to every item specified as part of
    the transaction, but we will assert the unchanged state of every
    non-modified object as part of the transaction. This allows
    building of transactions where the micro-verse of your transaction
    is known to be the fully consistent state as returned at the
    moment of the successful transaction completion, including items
    you did not need to modify.

    Returns the completed transaction, which contains the resulting
    items as written to the table(s).

    In the future we could add support for narrower ConditionChecks on
    unwritten items, rather than the present general assertion that
    their `item_version` was not changed.

    The default implementation for transact_write_items will also
    attempt to optimize your usage by reverting to a simple Put or
    Delete if you only operate on a single item.

    """
    batch_get_item, transact_write_items = boto3_impl_defaults(
        batch_get_item, transact_write_items,
    )

    built_transaction = None
    for i, _ in enumerate(attempts_iterator or timed_retry()):
        clean_transaction, built_transaction = _lazy_loading_transaction_builder(
            transaction_builder, item_keys_by_table_name, batch_get_item,
        )
        if built_transaction is clean_transaction or _is_empty(built_transaction):
            logger.info("No effects were defined, so the existing items will be returned as-is.")
            return clean_transaction

        try:
            transact_write_items(
                **built_transaction_to_transact_write_items_args(
                    built_transaction,
                    iso8601strict(datetime.utcnow()),
                    "",
                    item_version_attribute,
                    last_written_attribute,
                )
            )
            return built_transaction
        except ClientError as ce:
            # TODO in the future, have is_retryable determine which
            # specific items need to be refetched.  this would save us
            # some fetches in the general case.
            if not is_retryable(ce):
                raise
            logger.warning(f"Retrying attempt {i} that failed with {ce.response}")

        item_keys_by_table_name = all_items_for_next_attempt(built_transaction)

    assert built_transaction, "No attempt was made to run the transaction"
    raise TransactionAttemptsOverrun(f"Failed after {i + 1} attempts", built_transaction)
