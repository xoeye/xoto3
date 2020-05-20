import typing as ty
from datetime import datetime
from functools import partial
from logging import getLogger

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

from xoto3.tree_map import (
    map_tree,
    SimpleTransform,
    TreeTransform,
    type_dispatched_transform,
    make_path_only_transform,
    compose,
)

from xoto3.utils.dt import iso8601strict

from xoto3.dynamodb.types import InputItem, Item
from .decimal import float_to_decimal
from .truth import strip_falsy


logger = getLogger(__name__)

__ds = TypeDeserializer()
__sr = TypeSerializer()


def deserialize_item(d: dict) -> dict:
    """Dynamo has crazy serialization and they don't always get rid of it for us."""
    return {k: __ds.deserialize(d[k]) for k in d}


def serialize_item(d: dict) -> dict:
    return {k: __sr.serialize(d[k]) for k in d}


def old_dynamodb_stringset_fix(Set: set) -> ty.Optional[set]:
    """DynamoDB used to disallow the empty string within a StringSet"""
    if all(map(lambda x: isinstance(x, str) or x is None, Set)):
        # DynamoDB will not accept the empty string or None in a StringSet
        return {s for s in Set if s} or None  # don't ever return the empty set
    return Set


def dynamodb_prewrite_set_transform(Set: set) -> ty.Optional[set]:
    """DynamoDB will not accept Sets with empty strings, or empty Sets."""
    if not Set:
        # DynamoDB expects None if your set is empty
        return None
    # no guarantees your set is well-formed, but this covers some of the simple cases
    return Set


def dynamodb_prewrite_str_in_dict_transform(d: dict) -> dict:
    """DynamoDB will break if you try to provide an empty string as a
    String value of a key that is used as an index. It requires you to
    provide these attributes as None rather than the empty string.

    It _used_ to break if any attribute in any Map (nested or not) was the
    empty String. This behavior seems to have changed relatively recently.

    This function guards against this issue by simply replacing the
    empty string with None.

    """
    return {k: (v if not (isinstance(v, str) and not v) else None) for k, v in d.items()}


REQUIRED_TRANSFORMS: ty.Mapping[type, ty.Callable] = {
    float: float_to_decimal,
    # boto3 will yell if you provide floats instead of Decimals
    dict: make_path_only_transform(dynamodb_prewrite_str_in_dict_transform, ()),
    tuple: list,
    # boto3 expects lists rather than tuples
    set: dynamodb_prewrite_set_transform,
    # DynamoDB can reject various sorts of set writes; this is
    # intended to cover some of them for you.
}

STRONGLY_RECOMMENDED_TRANSFORMS: ty.Mapping[type, ty.Callable] = {
    dict: compose(
        make_path_only_transform(strip_falsy, ()),
        make_path_only_transform(dynamodb_prewrite_str_in_dict_transform, ()),
    ),
    # in many if not most cases, it's best not to store top-level attributes with falsy values.
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
