"""DynamoDB Streams record processing types and utilities"""
import typing as ty
from logging import getLogger

from typing_extensions import Literal

from xoto3.dynamodb.types import Item
from xoto3.dynamodb.utils.serde import deserialize_item

logger = getLogger(__name__)


class ItemCreated(ty.NamedTuple):
    old: Literal[None]
    new: Item


class ItemModified(ty.NamedTuple):
    old: Item
    new: Item


class ItemDeleted(ty.NamedTuple):
    old: Item
    new: Literal[None]


ItemImages = ty.Union[ItemCreated, ItemModified, ItemDeleted]
ExistingItemImages = ty.Union[ItemCreated, ItemModified]  # a common alias


def filter_existing(images_iter: ty.Iterable[ItemImages]) -> ty.Iterator[ExistingItemImages]:
    for item_images in images_iter:
        if item_images.new:
            yield item_images


def ddb_images(old: ty.Optional[Item], new: ty.Optional[Item]) -> ItemImages:
    if not old:
        assert new, "If old is not present then this should be a newly created item"
        return ItemCreated(None, new)
    if not new:
        assert old, "If new is not present then this should be a newly deleted item"
        return ItemDeleted(old, None)
    return ItemModified(old, new)


def old_and_new_items_from_stream_record_body(stream_record_body: dict) -> ItemImages:
    """If you're using the `records` wrapper this will get you what you need."""
    new = deserialize_item(stream_record_body.get("NewImage", {}))
    old = deserialize_item(stream_record_body.get("OldImage", {}))
    return ddb_images(old, new)


def old_and_new_items_from_stream_event_record(event_record: dict) -> ItemImages:
    """The event['Records'] list of dicts from a Dynamo stream as delivered to a Lambda
    can have each of its records processed individually by this function to deliver
    the two 'images' from the record.

    If the first item in the tuple is empty, there was no previous/old image,
    so this was an initial insert.

    If the second item in the tuple is empty, there is no new image, so this is a deletion.

    If both are present, this is an item update.
    """
    return old_and_new_items_from_stream_record_body(event_record["dynamodb"])


def old_and_new_dict_tuples_from_stream(event: dict) -> ty.List[ItemImages]:
    """Utility wrapper for old_and_new_items_from_stream_event_record"""
    tuples = [old_and_new_items_from_stream_event_record(record) for record in event["Records"]]
    logger.debug(f"Extracted {len(tuples)} stream records from the event.")
    return tuples
