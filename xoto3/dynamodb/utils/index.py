"""For looking at boto3 DynamoDB Index representations"""
from functools import partial
from logging import getLogger
from typing import Dict, Optional, Tuple

from xoto3.dynamodb.types import Index, KeyType, TableResource

logger = getLogger(__name__)


def _x_key_name(x: KeyType, index: Index) -> str:
    try:
        schema = index["KeySchema"]  # type: ignore
    except TypeError:
        schema = index
    for key in schema:
        if key["KeyType"] == x:
            return key["AttributeName"]
    return ""


hash_key_name = partial(_x_key_name, "HASH")
range_key_name = partial(_x_key_name, "RANGE")


def _indexes_by_keys(table: TableResource) -> Dict[Tuple[str, str], Index]:
    return {
        (hash_key_name(index), range_key_name(index)): index
        for indexes in [
            table.local_secondary_indexes,
            table.global_secondary_indexes,
            [table.key_schema],
        ]
        if indexes
        for index in indexes
    }


def find_index(table: TableResource, hash_key: str, range_key: str = "") -> Optional[Index]:
    """Find an index in a DynamoDB TableResource by known hash and optional range key.

    Returns None if no such index exists.
    """
    return _indexes_by_keys(table).get((hash_key, range_key))


def require_index(table: TableResource, hash_key: str, range_key: str = "") -> Index:
    """Raises if the index is not found. A common pattern."""
    index = find_index(table, hash_key, range_key)
    assert (
        index
    ), f"Index ({', '.join(filter(None,[hash_key, range_key]))}) was not found in table {table.name}"
    return index
