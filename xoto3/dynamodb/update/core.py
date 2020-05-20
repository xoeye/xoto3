import typing as ty
from logging import getLogger

from xoto3.dynamodb.types import TableResource, ItemKey, AttrDict, Item, InputItem
from .diff import build_update_diff, select_attributes_for_set_and_remove
from .builders import build_update
from .utils import logged_update_item


logger = getLogger(__name__)


def UpdateItem(
    Table: TableResource,
    Key: ItemKey,
    *,
    set_attrs: ty.Optional[AttrDict] = None,
    remove_attrs: ty.Collection[str] = (),
    add_attrs: ty.Optional[AttrDict] = None,
    delete_attrs: ty.Optional[AttrDict] = None,
    condition_exists: bool = True,
    **update_item_args,
) -> Item:
    update_args = build_update(
        Key,
        set_attrs=set_attrs,
        remove_attrs=remove_attrs,
        add_attrs=add_attrs,
        delete_attrs=delete_attrs,
        condition_exists=condition_exists,
        **update_item_args,
    )
    return logged_update_item(Table, Key, update_args)


def DiffedUpdateItem(
    Table: TableResource, Key: ItemKey, before: InputItem, after: InputItem, **kwargs
) -> InputItem:
    """Safe top-level diff update that requires only 'before' and 'after' dicts.

    By calling this you are trusting that we will make a choice about
    whether or not you actually have an update to perform.

    """
    item_diff = build_update_diff(before, after)
    if item_diff:
        logger.info(
            f"Updating item {Key} because there was an item diff.",
            extra=dict(json=dict(item_diff=item_diff)),
        )
        kwargs.pop("condition_exists", None)
        set_and_remove = select_attributes_for_set_and_remove(item_diff)
        return UpdateItem(
            Table,
            Key,
            set_attrs=set_and_remove["set_attrs"],
            remove_attrs=set_and_remove["remove_attrs"],
            condition_exists=True,
            **kwargs,
        )
    else:
        logger.debug(
            f"Not updating item {Key} because there was "
            "no meaningful difference between the items",
            extra=dict(json=dict(before=before, after=after)),
        )
        return before
