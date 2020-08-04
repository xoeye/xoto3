"""This implements a general-purpose wrapper for DynamoDB item updates

to prevent simultaneous read-write conflicts.
"""
import typing as ty
import copy
from datetime import datetime
import time
import random
import os
from logging import getLogger
from functools import partial

from typing_extensions import Protocol

from botocore.exceptions import ClientError

from xoto3.errors import client_error_name
from xoto3.utils.dt import iso8601strict
from xoto3.utils.tree_map import SimpleTransform
from xoto3.dynamodb.types import TableResource
from xoto3.dynamodb.get import strongly_consistent_get_item
from xoto3.dynamodb.types import ItemKey, Item, AttrDict
from .core import UpdateItem
from .diff import (
    build_update_diff,
    select_attributes_for_set_and_remove,
    _DEFAULT_PREDIFF_TRANSFORM,
)


DEFAULT_MAX_ATTEMPTS_BEFORE_FAILURE = 25
# this number is somewhat arbitrary, but infinite loops are very bad
MIN_TRANSACTION_SLEEP = 0.001
MAX_TRANSACTION_SLEEP = float(os.environ.get("DYNAMO_VERSIONING_RANDOM_SLEEP_SECONDS", 1.2))


logger = getLogger(__name__)


class VersionedUpdateFailure(Exception):
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
    item_id: ItemKey,
    *,
    get_item: ItemGetter = strongly_consistent_get_item,
    update_item: ItemUpdater = UpdateOrCreateItem,
    max_attempts_before_failure: int = DEFAULT_MAX_ATTEMPTS_BEFORE_FAILURE,
    item_version_key: str = "item_version",
    last_written_key: str = "last_written_at",
    random_sleep_on_lost_race: bool = True,
    prewrite_transform: ty.Optional[SimpleTransform] = _DEFAULT_PREDIFF_TRANSFORM,
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
    attempt = 0
    max_attempts_before_failure = int(max(1, max_attempts_before_failure))
    while attempt < max_attempts_before_failure:
        attempt += 1
        item = get_item(table, item_id)
        cur_item_version = item.get(item_version_key, 0)

        logger.debug(f"Current item version is {cur_item_version}")

        # do the incremental update
        updated_item = item_transformer(copy.deepcopy(item))
        if not updated_item:
            logger.debug("No transformed item was returned; returning original item")
            return item
        assert updated_item is not None
        item_diff = build_update_diff(item, updated_item, prediff_transform=prewrite_transform)
        if not item_diff:
            logger.info(
                "A transformed item was returned but no meaningful difference was found.",
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
                id_that_exists=next(iter(item_id.keys())) if item else "",
            )
            logger.debug(expr)
            update_item(table, item_id, **select_attributes_for_set_and_remove(item_diff), **expr)
            return updated_item
        except ClientError as ce:
            if client_error_name(ce) == "ConditionalCheckFailedException":
                msg = (
                    "Attempt %d to update item in table %s was beaten "
                    + "by a different update. Sleeping for %s seconds."
                )
                sleep = 0.0
                if random_sleep_on_lost_race:
                    sleep = random.uniform(MIN_TRANSACTION_SLEEP, MAX_TRANSACTION_SLEEP)
                    time.sleep(sleep)
                logger.warning(
                    msg,
                    attempt,
                    table.name,
                    f"{sleep:.3f}",
                    extra=dict(
                        json=dict(item_id=item_id, item_diff=item_diff, ce=str(ce), sleep=sleep)
                    ),
                )
            else:
                raise
    raise VersionedUpdateFailure(
        f"Failed to update item without performing overwrite {item_id}"
        f"Was beaten to the update {attempt} times."
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


def make_prefetched_get_item(item: Item, refetch_getter: ItemGetter = strongly_consistent_get_item):
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
        return refetch_getter(table, key)

    return prefetched_get_item
