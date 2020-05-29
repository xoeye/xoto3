import typing as ty
from datetime import datetime
from functools import partial
from logging import getLogger

from xoto3.utils.dt import iso8601strict
from xoto3.utils.dec import float_to_decimal
from xoto3.utils.tree_map import (
    map_tree,
    SimpleTransform,
    TreeTransform,
    type_dispatched_transform,
    make_path_only_transform,
    compose,
)

from .types import InputItem, Item
from .utils.truth import strip_falsy
from .utils.serde import (
    dynamodb_prewrite_set_transform,
    dynamodb_prewrite_empty_str_in_dict_to_null_transform,
)


logger = getLogger(__name__)


REQUIRED_TRANSFORMS: ty.Mapping[type, ty.Callable] = {
    float: float_to_decimal,
    # boto3 will yell if you provide floats instead of Decimals
    tuple: list,
    # boto3 expects lists rather than tuples
    set: dynamodb_prewrite_set_transform,
    # DynamoDB can reject various sorts of set writes; this is
    # intended to cover some of them for you.
}

_ACTIVE_UPDATE_TRANSFORM: SimpleTransform = partial(
    map_tree, type_dispatched_transform(REQUIRED_TRANSFORMS)
)
# when performing an update, we always need to run this transform no matter what,
# or boto3 or DynamoDB will be guaranteed to break on these types.

STRONGLY_RECOMMENDED_TRANSFORMS: ty.Mapping[type, ty.Callable] = {
    dict: compose(
        make_path_only_transform((), strip_falsy),
        make_path_only_transform((), dynamodb_prewrite_empty_str_in_dict_to_null_transform),
    ),
    # in many if not most cases, it's best not to store top-level attributes with falsy values at all.
    # this makes sparse secondary indexes possible and 'on by default'.
}

RECOMMENDED_TRANSFORMS: ty.Mapping[type, ty.Callable] = {
    datetime: iso8601strict,
    # you can of course choose your own datetime format if you like,
    # but boto3 will not allow you to leave datetimes untransformed.
}

OPTIONAL_TRANSFORMS = {dict: strip_falsy}

# this is the default prewrite transform
_ACTIVE_PREWRITE_TRANSFORM: SimpleTransform = partial(
    map_tree,
    type_dispatched_transform(
        {**REQUIRED_TRANSFORMS, **STRONGLY_RECOMMENDED_TRANSFORMS, **RECOMMENDED_TRANSFORMS}
    ),
)
# by default we use all of the recommended-and-above transforms as type-dispatched recursive transforms


def set_simple_dynamodb_prewrite_transform(transform: SimpleTransform):
    """You probably shouldn't use this, but if you want to completely
    override the built in shims with something of your own making, you
    can."""
    global _ACTIVE_PREWRITE_TRANSFORM
    logger.info("Setting new active DynamoDB prewrite transform ")
    prev = _ACTIVE_PREWRITE_TRANSFORM
    _ACTIVE_PREWRITE_TRANSFORM = transform
    return prev


def set_type_dispatched_dynamodb_prewrite_transforms(
    typed_transforms: ty.Mapping[type, TreeTransform]
):
    """If you want to customize the prewrite DynamoDB transform, this is likely what you want."""
    return set_simple_dynamodb_prewrite_transform(
        partial(map_tree, type_dispatched_transform(typed_transforms))
    )


def dynamodb_prewrite(item: InputItem, transform: ty.Optional[SimpleTransform] = None) -> Item:
    """All writes go through here no matter what.

    Uses the provided or default transforms to transform your item
    before handing it off to boto3 to be written to DynamoDB.

    If you want to adjust the behavior of writes, you need to set the transform one way or another.
    """
    if not transform:
        transform = _ACTIVE_PREWRITE_TRANSFORM
    return transform(item)
