import typing as ty
from copy import deepcopy

from xoto3.dynamodb.conditions import item_exists
from xoto3.dynamodb.exceptions import DynamoDbException
from xoto3.dynamodb.prewrite import dynamodb_prewrite, _ACTIVE_UPDATE_TRANSFORM
from xoto3.dynamodb.types import ItemKey, AttrDict
from xoto3.dynamodb.utils.expressions import make_unique_expr_attr_key


def build_update(
    Key: ItemKey,
    *,
    set_attrs: ty.Optional[AttrDict] = None,
    remove_attrs: ty.Collection[str] = (),
    add_attrs: ty.Optional[AttrDict] = None,
    delete_attrs: ty.Optional[AttrDict] = None,
    condition_exists: bool = True,
    **update_args,
) -> ty.Dict[str, ty.Any]:
    """Generates update_item argument dicts of medium complexity"""
    update_args = deepcopy(update_args)

    remove_attrs = set(remove_attrs)

    update_expression = ""
    expr_attr_names = update_args.get("ExpressionAttributeNames", dict())
    expr_attr_values = update_args.get("ExpressionAttributeValues", dict())
    if set_attrs:
        set_attrs = {k: v for k, v in set_attrs.items() if k not in Key}
        set_expr, eans, eavs = build_setattrs_for_update_item(set_attrs)
        update_expression += set_expr
        expr_attr_names.update(eans)
        expr_attr_values.update(eavs)

    if remove_attrs:
        remove_expr, eans = build_removeattrs_for_update(remove_attrs)
        update_expression += " " + remove_expr
        expr_attr_names.update(eans)

    if add_attrs:
        add_expr, eans, eavs = build_addattrs_for_update_item(add_attrs)
        update_expression += " " + add_expr
        expr_attr_names.update(eans)
        expr_attr_values.update(eavs)

    if delete_attrs:
        delete_expr, eans, eavs = build_deleteattrs_for_update_item(delete_attrs)
        update_expression += " " + delete_expr
        expr_attr_names.update(eans)
        expr_attr_values.update(eavs)

    update_args["UpdateExpression"] = update_expression
    if expr_attr_names:
        update_args["ExpressionAttributeNames"] = expr_attr_names
    if expr_attr_values:
        # if you provide empty set_attrs there will be nothing here!
        update_args["ExpressionAttributeValues"] = dynamodb_prewrite(
            expr_attr_values, _ACTIVE_UPDATE_TRANSFORM
        )
    update_args["Key"] = Key

    if "ReturnValues" not in update_args:
        update_args["ReturnValues"] = "ALL_NEW"

    if condition_exists:
        update_args = item_exists(Key)(update_args)

    return update_args


def build_setattrs_for_update_item(attrs_dict: dict) -> ty.Tuple[str, dict, dict]:
    """Utility for setting one or more attributes on a DynamoDB item.

    Creates a dictionary suitable for passing to a DynamoDB Table
    resource's update_item method.

    Removes keys from the attrs_dict for you, since those are not
    valid for updates.

    """
    if not attrs_dict:
        raise DynamoDbException("Cannot perform an update with no attributes!")

    set_expr = "SET "
    expr_attr_names: ty.Dict[str, str] = dict()
    expr_attr_values = dict()
    for attrname, value in attrs_dict.items():
        key = make_unique_expr_attr_key(attrname, set(expr_attr_names))
        set_expr += f"#{key} = :{key}, "
        expr_attr_names[f"#{key}"] = attrname
        expr_attr_values[f":{key}"] = value
    set_expr = set_expr.rstrip(", ")

    return set_expr, expr_attr_names, expr_attr_values


def build_addattrs_for_update_item(attrs_dict: dict) -> ty.Tuple[str, dict, dict]:
    add_expr = "ADD "
    ea_names: ty.Dict[str, str] = dict()
    ea_values = dict()
    for attrname, value in attrs_dict.items():
        key = make_unique_expr_attr_key(attrname, set(ea_names))
        add_expr += f"#{key} :add{key}, "
        ea_names[f"#{key}"] = attrname
        ea_values[f":add{key}"] = value
    add_expr = add_expr.rstrip(", ")
    return add_expr, ea_names, ea_values


def build_deleteattrs_for_update_item(attrs_dict: dict) -> ty.Tuple[str, dict, dict]:
    del_expr = "DELETE "
    ea_names: ty.Dict[str, str] = dict()
    ea_values = dict()
    for attrname, value in attrs_dict.items():
        key = make_unique_expr_attr_key(attrname, set(ea_names))
        del_expr += f"#{key} :del{key}, "
        ea_names[f"#{key}"] = attrname
        ea_values[f":del{key}"] = value
    del_expr = del_expr.rstrip(", ")
    return del_expr, ea_names, ea_values


def build_removeattrs_for_update(attr_names: ty.Collection) -> ty.Tuple[str, dict]:
    expr_attr_names = {
        f"#{make_unique_expr_attr_key(attrname)}": attrname for attrname in attr_names
    }
    remove_expr = "REMOVE " + ", ".join(key for key in set(expr_attr_names))
    return remove_expr, expr_attr_names
