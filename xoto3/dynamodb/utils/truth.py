from typing import Any


def strip_falsy(d: dict) -> dict:
    """Strip falsy, but don't strip integer value 0"""
    return {key: val for key, val in d.items() if dynamo_truthy(val)}


def dynamo_truthy(val: Any) -> bool:
    """Attribute values that do not pass this test should generally not be
    stored in DynamoDB - instead, their attribute key should be
    removed from the object altogether.

    Empty strings and empty sets in particular will fail this test,
    and they must *always* be stripped - the alternative is to store
    an explicit NULL for the key, but in most cases, it is more
    efficient for application code to assume these NULLs upon read
    rather than writing them explicitly to the database.

    The additional advantage of stripping these null values is that
    every secondary index will be a sparse index by default.
    """
    return bool(val) or (val == 0 and not isinstance(val, bool))


def dynamo_meaningful_value_update(old_val: Any, new_val: Any) -> bool:
    """If both an existing/old and a new value are 'effectively NULL' from
    a Dymamo perspective, then this does not represent a meaningful update
    to the database and may be dropped.
    """
    return (dynamo_truthy(old_val) or dynamo_truthy(new_val)) and new_val != old_val
