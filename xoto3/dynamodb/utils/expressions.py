import hashlib
import string
import os


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


def validate_attr_key(attr_name: str):
    if _filter_alphanum(attr_name) != attr_name:
        raise ValueError(f"Attribute name contains invalid characters: '{attr_name}'")


def add_variables_to_expression(query_dict: dict, variables: dict) -> dict:
    """Attempt to make it easier to develop a query"""
    ea_names = query_dict.get("ExpressionAttributeNames", {})
    ea_values = query_dict.get("ExpressionAttributeValues", {})
    for k, v in variables.items():
        validate_attr_key(k)
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
