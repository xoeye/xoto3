"""This implements a general-purpose wrapper for DynamoDB item updates

to prevent simultaneous read-write conflicts.
"""
import copy
import os
import random
import time
import typing as ty
from datetime import datetime
from functools import partial
from logging import getLogger

from botocore.exceptions import ClientError
from typing_extensions import Protocol

from xoto3.dynamodb.constants import DEFAULT_ITEM_NAME
from xoto3.dynamodb.exceptions import DynamoDbItemException, get_item_exception_type
from xoto3.dynamodb.get import (
    GetItem,
    strongly_consistent_get_item,
    strongly_consistent_get_item_if_exists,
)
from xoto3.dynamodb.types import AttrDict, Item, ItemKey, TableResource
from xoto3.errors import client_error_name
from xoto3.utils.dt import iso8601strict
from xoto3.utils.tree_map import SimpleTransform

from .core import UpdateItem
from .diff import (
    _DEFAULT_PREDIFF_TRANSFORM,
    build_update_diff,
    select_attributes_for_set_and_remove,
)

DEFAULT_MAX_ATTEMPTS_BEFORE_FAILURE = 25
# this number is somewhat arbitrary, but infinite loops are very bad
MIN_TRANSACTION_SLEEP = 0.001
MAX_TRANSACTION_SLEEP = float(os.environ.get("DYNAMO_VERSIONING_RANDOM_SLEEP_SECONDS", 1.2))


logger = getLogger(__name__)


class VersionedUpdateFailure(DynamoDbItemException):
    pass


ItemGetter = ty.Callable[[TableResource, ItemKey], Item]
"""a callable taking a TableResource and ItemKey and returning the Item"""

ItemTransformer = ty.Callable[[Item], ty.Optional[Item]]
"""a callable taking the current item and returning the modified version you wish to store in DynamoDB"""


class ItemUpdater(Protocol):
    """Matches UpdateItem"""

    def __call__(
        self,
        Table: TableResource,
        Key: ItemKey,
        *,
        set_attrs: ty.Optional[AttrDict] = None,
        remove_attrs: ty.Collection[str] = (),
        add_attrs: ty.Optional[AttrDict] = None,
        delete_attrs: ty.Optional[AttrDict] = None,
        **update_args,
    ) -> Item:
        ...


UpdateOrCreateItem = partial(UpdateItem, condition_exists=False)


def versioned_diffed_update_item(
    table: TableResource,
    item_transformer: ItemTransformer,
    item_key: ItemKey = None,
    *,
    get_item: ItemGetter = strongly_consistent_get_item,
    update_item: ItemUpdater = UpdateOrCreateItem,
    max_attempts_before_failure: int = DEFAULT_MAX_ATTEMPTS_BEFORE_FAILURE,
    item_version_key: str = "item_version",
    last_written_key: str = "last_written_at",
    random_sleep_on_lost_race: bool = True,
    prewrite_transform: ty.Optional[SimpleTransform] = _DEFAULT_PREDIFF_TRANSFORM,
    item_id: ItemKey = None,  # deprecated name, present for backward-compatibility
    nicename: str = DEFAULT_ITEM_NAME,
) -> Item:
    """Performs an item read-transform-write loop until there are no intervening writes.

    By swapping out the get_item implementation, you can repurpose
    this from only allowing updates to existing items to a
    transactional create. Your get_item must simply return an empty
    dict if the item does not already exist.  The built-in
    get_item, strongly_consistent_get_item, raises an Exception if
    the item is not found, making the default behavior update-only.

    Another way to repurpose the general transactional behavior
    provided here is with make_single_reuse_get_item: if you
    already have the item in question (e.g. because of a BatchGet) and
    want to make a transactional update to it without incurring a
    useless fetch, you can swap out get_item for a closure created
    by that function that will return your existing item once, but
    will revert to fetching if the transaction fails because of an
    intervening write.
    """
    item_key = item_key or item_id
    assert item_key, "Must pass item_key or (deprecated) item_id"

    attempt = 0
    max_attempts_before_failure = int(max(1, max_attempts_before_failure))
    update_arguments = None

    nice_get_item = _nicename_getter(nicename, get_item)

    while attempt < max_attempts_before_failure:
        attempt += 1
        item = nice_get_item(table, item_key)
        cur_item_version = item.get(item_version_key, 0)

        logger.debug(f"Current item version is {cur_item_version}")

        # do the incremental update
        updated_item = item_transformer(copy.deepcopy(item))
        if not updated_item:
            logger.debug(f"No transformed {nicename} was returned; returning original {nicename}")
            return item
        assert updated_item is not None
        item_diff = build_update_diff(item, updated_item, prediff_transform=prewrite_transform)
        if not item_diff:
            logger.info(
                f"Transformed {nicename} was returned but no meaningful difference was found.",
                extra=dict(json=dict(item=item, updated_item=updated_item)),
            )
            return item

        # set incremented item_version and last_written_at on the item_diff
        # and the updated_item - the former will be sent to DynamoDB, the latter
        # returned to the user.
        item_diff[item_version_key] = int(cur_item_version) + 1
        item_diff[last_written_key] = iso8601strict(datetime.utcnow())
        updated_item[item_version_key] = item_diff[item_version_key]
        updated_item[last_written_key] = item_diff[last_written_key]

        try:
            # write if no intervening updates
            expr = versioned_item_expression(
                cur_item_version,
                item_version_key,
                id_that_exists=next(iter(item_key.keys())) if item else "",
            )
            logger.debug(expr)
            update_arguments = select_attributes_for_set_and_remove(item_diff)
            # store arguments for later logging
            update_item(table, item_key, **update_arguments, **expr)
            return updated_item
        except ClientError as ce:
            if client_error_name(ce) == "ConditionalCheckFailedException":
                msg = (
                    "Attempt %d to update %s in table %s was beaten "
                    + "by a different update. Sleeping for %s seconds."
                )
                sleep = 0.0
                if random_sleep_on_lost_race:
                    sleep = random.uniform(MIN_TRANSACTION_SLEEP, MAX_TRANSACTION_SLEEP)
                    time.sleep(sleep)
                logger.warning(
                    msg,
                    attempt,
                    nicename,
                    table.name,
                    f"{sleep:.3f}",
                    extra=dict(
                        json=dict(item_key=item_key, item_diff=item_diff, ce=str(ce), sleep=sleep,)
                    ),
                )
            else:
                raise
    raise get_item_exception_type(nicename, VersionedUpdateFailure)(
        f"Failed to update {nicename} without performing overwrite {item_key}. "
        f"Was beaten to the update {attempt} times.",
        key=item_key,
        table_name=table.name,
        update_arguments=update_arguments,
    )


# this could be used in a put_item scenario as well, or even with a batch_writer
def versioned_item_expression(
    item_version: int, item_version_key: str = "item_version", id_that_exists: str = ""
) -> dict:
    """Assembles a DynamoDB ConditionExpression with ExprAttrNames and
    Values that will ensure that you are the only caller of
    versioned_item_diffed_update that has updated this item.

    In general it would be a silly thing to not pass id_that_exists if
    your item_version is not also 0.  However, since this is just a
    helper function and is only used (currently) by the local consumer
    versioned_item_diffed_update, there is no need to enforce this.

    """
    expr_names = {"#itemVersion": item_version_key}
    expr_vals = {":curItemVersion": item_version}
    item_version_condition = "#itemVersion = :curItemVersion"
    first_time_version_condition = "attribute_not_exists(#itemVersion)"
    if id_that_exists:
        expr_names["#idThatExists"] = id_that_exists
        first_time_version_condition = (
            f"( {first_time_version_condition} AND attribute_exists(#idThatExists) )"
        )
    return dict(
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
        ConditionExpression=item_version_condition + " OR " + first_time_version_condition,
    )


def make_prefetched_get_item(
    item: Item,
    refetch_getter: ItemGetter = strongly_consistent_get_item,
    *,
    nicename: str = DEFAULT_ITEM_NAME,
):
    """Useful for versioned updates where you've already fetched the item
    once and in most cases would not need to fetch again before running
    the update, but would want a versioned update to retry with a fresh
    get if the item had been updated before your update completed.
    """
    used = False

    def prefetched_get_item(table, key: ItemKey) -> Item:
        nonlocal used

        if not used:
            used = True
            return item
        return _nicename_getter(nicename, refetch_getter)(table, key)

    return prefetched_get_item


def _nicename_getter(nicename: str, get_item: ItemGetter) -> ItemGetter:
    """I didn't write the ItemGetter Protocol to handle the possibility of
    keyword arguments, which means that the nicename as supplied to
    versioned_diffed_update_item cannot be used in the context of the
    call to the caller-supplied get_item implementation.

    In the default case, and in cases where the getter
    supplied is defined by us, we can safely use nicename, which
    provides helpful additional context if the Item is not found.

    Alternative approaches include EAFP (try, catch TypeError, retry
    without nicename) as well as using `inspect` to dynamically assess
    whether the keyword argument would be accepted.

    This should be more performant than EAFP in the failure case,
    nearly as performant in the success case, and (this is the main
    thing) doesn't require another function to enter the stack trace
    for an exception (such as ItemNotFoundException).

    Inspect is rejected because it's expensive. _Technically_ it would
    be possible to amortize the cost of inspect by caching the
    results, and that's a little bit tempting, but if the callers are
    using partial application then they might be recreating the
    function every time and we'd have to limit the cache size, etc....

    Overall, it seems simplest to try to add some helpful sugar/info
    in the standard cases, and let callers do their own work for cases
    where they're throwing their own exceptions or doing other fancy
    stuff.

    """
    if (
        get_item is strongly_consistent_get_item
        or get_item is GetItem
        or get_item is strongly_consistent_get_item_if_exists
    ):
        return partial(get_item, nicename=nicename)
    return get_item
