from typing import Union
from copy import deepcopy

from .types import PrimaryIndex, ItemKey
from .utils.index import hash_key_name


def condition_attribute_exists(attribute_name: str) -> dict:
    return dict(ConditionExpression=f"attribute_exists({attribute_name})")


def any_key_name(key: ItemKey) -> str:
    return list(key.keys())[0]


def item_exists(key_or_schema: Union[ItemKey, PrimaryIndex]) -> str:
    key_name = (
        any_key_name(key_or_schema)
        if isinstance(key_or_schema, dict)
        else hash_key_name(key_or_schema)
    )
    return f"attribute_exists({key_name})"


def item_not_exists(key_or_schema: Union[ItemKey, PrimaryIndex]) -> str:
    key_name = (
        any_key_name(key_or_schema)
        if isinstance(key_or_schema, dict)
        else hash_key_name(key_or_schema)
    )
    return f"attribute_not_exists({key_name})"


def and_condition(args_dict: dict, condition: str) -> dict:
    with_condition_expression = deepcopy(args_dict)
    if "ConditionExpression" in with_condition_expression:
        with_condition_expression["ConditionExpression"] += " AND " + condition
    else:
        with_condition_expression["ConditionExpression"] = condition
    return with_condition_expression
