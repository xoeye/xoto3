import hashlib
import os
import string

_HASH_LEN = int(os.environ.get("XOTO3_EXPR_ATTR_HASH_LENGTH", 8))
# if you have some reason to be concerned about hash collisions you can always
# set this to make your DynamoDB expression attribute names/values more verbose.


def _filter_alphanum(s: str) -> str:
    return "".join(c for c in s if c in string.ascii_letters or c in string.digits or c == "_")


def make_unique_expr_attr_key(attr_name: str) -> str:
    clean = _filter_alphanum(attr_name)
    if clean == attr_name:
        return clean
    hashed = hashlib.sha256(attr_name.encode())
    return clean + "__xoto3__" + hashed.hexdigest()[:_HASH_LEN]


def add_variables_to_expression(query_dict: dict, variables: dict) -> dict:
    """Attempt to make it easier to develop a query"""
    ea_names = query_dict.get("ExpressionAttributeNames", {})
    ea_values = query_dict.get("ExpressionAttributeValues", {})
    for k, v in variables.items():
        name = f"#{k}"
        if name in ea_names:
            raise ValueError(
                f"Cannot add a duplicate expression attribute "
                f"name {name} to your query {query_dict}"
            )
        ea_names[name] = k
        name = f":{k}"
        if name in ea_values:
            raise ValueError(
                f"Cannot add a duplicate expression attribute "
                f"value {name} to your query {query_dict}"
            )
        ea_values[name] = v
    query_dict["ExpressionAttributeNames"] = ea_names
    query_dict["ExpressionAttributeValues"] = ea_values
    return query_dict


# this could be used in a put_item scenario as well, or even with a batch_writer
def versioned_item_expression(
    item_version: int, item_version_key: str = "item_version", id_that_exists: str = ""
) -> dict:
    """Assembles a DynamoDB ConditionExpression with ExprAttrNames and
    Values that will ensure that you are the only caller of
    versioned_item_diffed_update that has updated this item.

    In general it would be a silly thing to not pass id_that_exists if
    your item_version is not also 0.  However, since this is just a
    helper function and is only used (currently) by the local consumer
    versioned_item_diffed_update, there is no need to enforce this.

    """
    expr_names = {"#itemVersion": item_version_key}
    expr_vals = {":curItemVersion": item_version}
    item_version_condition = "#itemVersion = :curItemVersion"
    first_time_version_condition = "attribute_not_exists(#itemVersion)"
    if id_that_exists:
        expr_names["#idThatExists"] = id_that_exists
        first_time_version_condition = (
            f"( {first_time_version_condition} AND attribute_exists(#idThatExists) )"
        )
    return dict(
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_vals,
        ConditionExpression=item_version_condition + " OR " + first_time_version_condition,
    )
