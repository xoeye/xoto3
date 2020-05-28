"""For looking at boto3 DynamoDB Index representations"""
from typing import Optional
from functools import partial
from logging import getLogger

from xoto3.dynamodb.types import TableResource, Index


logger = getLogger(__name__)


def _x_key_name(x: str, index: Index) -> str:
    try:
        schema = index["KeySchema"]  # type: ignore
    except TypeError:
        schema = index
    for key in schema:
        if key["KeyType"] == x:
            return key["AttributeName"]
    logger.error(f"No {x} key in index {index}")
    raise ValueError(f"No {x} key in index")


hash_key_name = partial(_x_key_name, "HASH")
range_key_name = partial(_x_key_name, "RANGE")


def find_index(table: TableResource, hash_key: str, range_key: str) -> Optional[Index]:
    """Find an index in a DynamoDB TableResource by known hash and range key.

    Returns None if no such index exists.
    """
    for indexes in [
        table.local_secondary_indexes,
        table.global_secondary_indexes,
        [table.key_schema],
    ]:
        if indexes:
            for index in indexes:
                if hash_key_name(index) == hash_key and range_key_name(index) == range_key:
                    return index
    return None


def require_index(table: TableResource, hash_key: str, range_key: str) -> Index:
    """Raises if the index is not found. A common pattern."""
    index = find_index(table, hash_key, range_key)
    assert index, f"Index ({hash_key}, {range_key}) was not found in table {table.name}"
    return index
