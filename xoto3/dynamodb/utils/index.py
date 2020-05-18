"""For looking at boto3 DynamoDB Index representations"""
from typing import Optional
from functools import partial

from xoipy.log import get_logger
from .types import TableResource, DynamoIndex


logger = get_logger(__name__)


def _x_key_name(x: str, index: DynamoIndex) -> str:
    for key in index["KeySchema"]:
        if key["KeyType"] == x:
            return key["AttributeName"]
    logger.error(f"No {x} key in index {index}")
    raise ValueError(f"No {x} key in index")


hash_key_name = partial(_x_key_name, "HASH")
range_key_name = partial(_x_key_name, "RANGE")


def find_index(table: TableResource, hash_key: str, range_key: str) -> Optional[DynamoIndex]:
    """Find an index in a DynamoDB TableResource by known hash and range key.

    Returns None if no such index exists.
    """
    for indexes in [table.local_secondary_indexes, table.global_secondary_indexes]:
        if indexes:
            for index in indexes:
                if hash_key_name(index) == hash_key and range_key_name(index) == range_key:
                    return index
    return None
