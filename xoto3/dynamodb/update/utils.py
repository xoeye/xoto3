import typing as ty
from logging import getLogger

from xoto3.dynamodb.types import TableResource, ItemKey, Item


logger = getLogger(__name__)


def logged_update_item(
    Table: TableResource, Key: ItemKey, update_args: ty.Mapping[str, ty.Any]
) -> Item:
    """A logged wrapper for Table.update_item"""
    try:
        dyn_resp = Table.update_item(**update_args)
        if update_args.get("ReturnValues", "NONE") != "NONE":
            return make_item_dict_from_updateItem_response(Key, dyn_resp)
        return dict()
    except Exception as e:
        # verbose logging if an error occurs
        logger.info("UpdateItem arguments", extra=dict(json=dict(update_args)))
        e.update_item_arguments = update_args  # type: ignore
        raise e


def make_item_dict_from_updateItem_response(item_key: ItemKey, update_resp: dict) -> dict:
    """Simple utility for response to update_item where you want to return
    the full object, which is a common pattern."""
    return {**item_key, **update_resp["Attributes"]}
