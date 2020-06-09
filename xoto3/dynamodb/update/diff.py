from typing import Dict, Any, Set, Optional
from functools import partial
from logging import getLogger
from typing_extensions import TypedDict

from xoto3.dynamodb.types import InputItem, AttrDict, AttrInput
from xoto3.dynamodb.utils.truth import dynamodb_truthy
from xoto3.dynamodb.prewrite import (
    map_tree,
    type_dispatched_transform,
    REQUIRED_TRANSFORMS,
    RECOMMENDED_TRANSFORMS,
    SimpleTransform,
)


logger = getLogger(__name__)


_DEFAULT_PREDIFF_TRANSFORM = partial(
    map_tree, type_dispatched_transform({**REQUIRED_TRANSFORMS, **RECOMMENDED_TRANSFORMS})
)


def build_update_diff(
    old: InputItem,
    new: InputItem,
    *,
    prediff_transform: Optional[SimpleTransform] = _DEFAULT_PREDIFF_TRANSFORM,
) -> AttrDict:
    """Gets an update dict for Dynamo that is the meaningful top-level
    attribute Dynamo-specific difference between the two items,
    assuming your items support deep (recursive) equality checks.

    By 'meaningful' we mean that we're not going to waste time/money
    updating an item if all you've done is changed some attributes
    from a 'basically empty' value such as None to another 'basically
    empty' value such as an empty string set.

    This is done in the hopes of making it easy for developers to get
    semantically useful things done without stressing over the details
    of how Dynamo works.

    """
    if prediff_transform:
        new = prediff_transform(new)

    diff: Dict[str, Any] = dict()
    for key in new.keys():
        new_val = new[key]
        old_val = old.get(key, None)
        if is_meaningful_value_update(old_val, new_val):
            diff[key] = new_val
    for key in set(old.keys()) - set(new.keys()):
        # even if the old value was not Dynamo-truthy, if a caller has
        # removed the key itself that could be a sign that they want
        # to remove this item from a sparse index, so we will consider
        # this to be a meaningful update even though it doesn't modify
        # the data itself in an otherwise meaningful way.
        diff[key] = None
    return diff


def is_meaningful_value_update(old_val: Any, new_val: Any) -> bool:
    """If both an existing/old and a new value are 'effectively NULL' from
    a DynamoDB perspective, then this does not represent a meaningful update
    to the database and may be dropped.
    """
    return (dynamodb_truthy(old_val) or dynamodb_truthy(new_val)) and new_val != old_val


SetAndRemoveDict = TypedDict("SetAndRemoveDict", {"set_attrs": AttrDict, "remove_attrs": Set[str]})


def select_attributes_for_set_and_remove(d: AttrInput) -> SetAndRemoveDict:
    """Returns a dict with keys suitable for passing to UpdateItem.

    May be used on the output of construct_update_diff to prepare a call to UpdateItem.
    """
    set_attrs: AttrDict = dict()
    remove_attrs: Set[str] = set()
    for key, value in d.items():
        if dynamodb_truthy(value):
            set_attrs[key] = value
        else:
            remove_attrs.add(key)
    if set_attrs or remove_attrs:
        setting = f"setting {list(set_attrs.keys())} " if set_attrs else ""
        removing = f"removing {list(remove_attrs)} " if remove_attrs else ""
        logger.debug(setting + removing)
    return dict(set_attrs=set_attrs, remove_attrs=remove_attrs)
