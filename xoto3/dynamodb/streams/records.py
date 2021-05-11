"""DynamoDB Streams record processing types and utilities"""
import typing as ty
from logging import getLogger

from typing_extensions import Literal

from xoto3.dynamodb.types import Item, ItemKey
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


def item_images(old: ty.Optional[Item], new: ty.Optional[Item]) -> ItemImages:
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
    return item_images(old, new)


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
    """Logging wrapper for a whole stream event. You probably don't want to use this."""
    images = [old_and_new_items_from_stream_event_record(record) for record in event["Records"]]
    logger.debug(f"Extracted {len(images)} stream records from the stream event.")
    return images


def matches_key(item_key: ItemKey) -> ty.Callable[[ItemImages], bool]:
    if not item_key:
        raise ValueError("Empty item key")

    def _matches_key(images: ItemImages) -> bool:
        """a filter function"""
        old, new = images
        for k, kv in item_key.items():
            if old and not old.get(k) == kv:
                return False
            if new and not new.get(k) == kv:
                return False
        return True

    return _matches_key


def filter_existing(images_iter: ty.Iterable[ItemImages]) -> ty.Iterator[ExistingItemImages]:
    for item_images in images_iter:
        if item_images.new:
            yield item_images


def current_nonempty_value(
    item_key: ItemKey,
) -> ty.Callable[[ty.Iterable[ItemImages]], ty.Iterator[Item]]:
    """We're only interested in a stream of the current values for a
    particular item, and we're not interested in deletions.
    """
    item_matcher = matches_key(item_key)

    def item_only_if_it_exists(images_stream: ty.Iterable[ItemImages]) -> ty.Iterator[Item]:
        for existing_item_images in filter_existing(images_stream):
            if item_matcher(existing_item_images):
                yield existing_item_images.new

    return item_only_if_it_exists
