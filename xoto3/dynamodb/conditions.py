from typing import Union, Iterable
from copy import deepcopy
from random import choice
import string

from .types import PrimaryIndex, ItemKey
from .utils.index import hash_key_name


def condition_attribute_exists(attribute_name: str) -> dict:
    """You're probably better off with the more composable add_condition_attribute_exists"""
    return dict(ConditionExpression=f"attribute_exists({attribute_name})")


def add_condition_attribute_exists(attribute_name: str):
    return and_named_condition("attribute_exists({name})", attribute_name)


def add_condition_attribute_not_exists(attribute_name: str):
    return and_named_condition("attribute_not_exists({name})", attribute_name)


def _any_key_name(key: ItemKey) -> str:
    return list(key.keys())[0]


def _get_key_name(key_or_schema: Union[ItemKey, PrimaryIndex]):
    return (
        _any_key_name(key_or_schema)
        if isinstance(key_or_schema, dict)
        else hash_key_name(key_or_schema)
    )


def _range_str(start: str) -> Iterable[str]:
    suffix = ""
    while True:
        yield start + suffix
        suffix += choice(string.ascii_lowercase)


def and_named_condition(condition_fmt: str, name: str, *, ex_attr_name: str = "#_anc_name"):
    """Constructs a composable query transformer, which will itself add to
    an existing ConditionExpression if present."""
    assert "name" in condition_fmt, "Format string must contain 'name'"
    assert ex_attr_name.startswith("#"), "Expression attribute names must start with #"

    def and_condition_expr(args: dict) -> dict:
        """Concatenates a ConditionExpression on the named attribute with any
        existing ConditionExpression in the given request dict.
        """
        args = deepcopy(args)
        existing_names = args.get("ExpressionAttributeNames", dict())
        for ex_n in _range_str(ex_attr_name):
            # find an unused expression attribute name
            if ex_n not in existing_names:
                break

        names = {ex_n: name}
        cond_expr = condition_fmt.format(name=ex_n)
        args["ExpressionAttributeNames"] = {**existing_names, **names}
        return and_condition(args, cond_expr)

    return and_condition_expr


def item_exists(key_or_schema: Union[ItemKey, PrimaryIndex]):
    return add_condition_attribute_exists(_get_key_name(key_or_schema))


def item_not_exists(key_or_schema: Union[ItemKey, PrimaryIndex]):
    return add_condition_attribute_not_exists(_get_key_name(key_or_schema))


def and_condition(args_dict: dict, condition: str) -> dict:
    with_condition_expression = deepcopy(args_dict)
    if "ConditionExpression" in with_condition_expression:
        with_condition_expression["ConditionExpression"] += " AND " + condition
    else:
        with_condition_expression["ConditionExpression"] = condition
    return with_condition_expression
