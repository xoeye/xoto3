"""This is intended as a more intuitive method of building basic DynamoDB queries.

All of these functional query builders assume that you will start with single_partition.
"""
from typing import Dict, Any, Optional
from copy import deepcopy

from .types import Index, KeyAttributeType
from .utils.index import hash_key_name, range_key_name


TableQuery = Dict[str, Any]


def single_partition(index: Index, partition_value: KeyAttributeType) -> TableQuery:
    """Sets up a simple query/scan dict for a single partition which can
    be provided to a boto3 TableResource.

    This is the core of any DynamoDB query; you cannot query anything but a single partition.

    Only indices (whether primary or secondary) with a composite key may be queried.
    """
    query: Dict[str, Any] = dict()
    try:
        query["IndexName"] = index["IndexName"]  # type: ignore
    except TypeError:
        pass  # a primary index

    keystr = "#partition"
    valstr = ":partition"
    query["KeyConditionExpression"] = f"{keystr} = {valstr} "
    query["ExpressionAttributeNames"] = {keystr: hash_key_name(index)}
    query["ExpressionAttributeValues"] = {valstr: partition_value}

    return query


def order(ascending: bool):
    """Creates a query builder"""

    def tx_query(query: TableQuery) -> TableQuery:
        """Creates new query with ScanIndexForward set."""
        return dict(query, ScanIndexForward=ascending)

    return tx_query


ascending = order(ascending=True)
descending = order(ascending=False)


def limit(limit: int):
    def tx_query(query: TableQuery) -> TableQuery:
        return dict(query, Limit=limit) if limit else query

    return tx_query


def From(last_evaluated_key: str):
    def tx_query(query: TableQuery) -> TableQuery:
        return dict(query, ExclusiveStartKey=last_evaluated_key) if last_evaluated_key else query

    return tx_query


def within_range(
    index: Index, gte: Optional[KeyAttributeType] = None, lte: Optional[KeyAttributeType] = None
):
    by = range_key_name(index)

    expr_attr_names = dict()
    expr_attr_values = dict()
    key_condition_expr = ""

    if gte and lte:
        expr_attr_names["#sortBy"] = by
        expr_attr_values[":GTE"] = gte
        expr_attr_values[":LTE"] = lte
        key_condition_expr += f" AND #sortBy BETWEEN :GTE and :LTE"
    elif gte:
        expr_attr_names["#sortBy"] = by
        expr_attr_values[":GTE"] = gte
        key_condition_expr += f" AND #sortBy >= :GTE "
    elif lte:
        expr_attr_names["#sortBy"] = by
        expr_attr_values[":LTE"] = lte
        key_condition_expr += f" AND #sortBy <= :LTE "

    def tx_query(query: TableQuery) -> TableQuery:
        query = deepcopy(query)
        query["ExpressionAttributeNames"] = dict(
            query["ExpressionAttributeNames"], **expr_attr_names
        )
        query["ExpressionAttributeValues"] = dict(
            query["ExpressionAttributeValues"], **expr_attr_values
        )
        if key_condition_expr:
            query["KeyConditionExpression"] += key_condition_expr
        return query

    return tx_query