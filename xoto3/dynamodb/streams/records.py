"""Dynamo Streams record processing"""
import typing as ty

from logging import getLogger

from xoto3.dynamodb.utils.serde import deserialize_item
from xoto3.dynamodb.types import Item


logger = getLogger(__name__)


def old_and_new_items_from_stream_event_record(event_record: dict,) -> ty.Tuple[Item, Item]:
    """The event['Records'] list of dicts from a Dynamo stream as delivered to a Lambda
    can have each of its records processed individually by this function to deliver
    the two 'images' from the record.

    If the first item in the tuple is empty, there was no previous/old image,
    so this was an initial insert.

    If the second item in the tuple is empty, there is no new image, so this is a deletion.

    If both are present, this is an item update.
    """
    return old_and_new_items_from_stream_record_body(event_record["dynamodb"])


def old_and_new_dict_tuples_from_stream(event: dict) -> ty.List[ty.Tuple[Item, Item]]:
    """Utility wrapper for old_and_new_items_from_stream_event_record"""
    tuples = [old_and_new_items_from_stream_event_record(record) for record in event["Records"]]
    logger.debug(f"Extracted {len(tuples)} stream records from the event.")
    return tuples


def old_and_new_items_from_stream_record_body(stream_record_body: dict,) -> ty.Tuple[Item, Item]:
    """If you're using the `records` wrapper this will get you what you need."""
    new = deserialize_item(stream_record_body.get("NewImage", {}))
    old = deserialize_item(stream_record_body.get("OldImage", {}))
    return old, new
