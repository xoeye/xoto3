"""This is intended as a more intuitive method of building basic DynamoDB queries.

All of these functional query builders assume that you will start with single_partition.
"""
from copy import deepcopy
from functools import partial
from typing import Any, Callable, Dict, Optional, Tuple

from .types import Index, KeyAttributeType, TableQuery
from .utils.index import (  # noqa # included only for cleaner imports
    find_index,
    hash_key_name,
    range_key_name,
    require_index,
)


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


QueryTransformer = Callable[[TableQuery], TableQuery]


def order(ascending: bool) -> QueryTransformer:
    """Creates a query builder"""

    def tx_query(query: TableQuery) -> TableQuery:
        """Creates new query with ScanIndexForward set."""
        return dict(query, ScanIndexForward=ascending)

    return tx_query


ascending = order(ascending=True)
descending = order(ascending=False)


def limit(limit: int) -> QueryTransformer:
    def tx_query(query: TableQuery) -> TableQuery:
        return dict(query, Limit=limit) if limit else query

    return tx_query


def page(last_evaluated_key: dict) -> QueryTransformer:
    """Resume a query on the page represented by the LastEvaluatedKey you
    previously received.

    Note that there are pagination utilities in `paginate` if you don't
    actually need to maintain this state (e.g., across client calls in a
    RESTful service) and simply want to iterate through all the results.
    """

    def tx_query(query: TableQuery) -> TableQuery:
        return dict(query, ExclusiveStartKey=last_evaluated_key) if last_evaluated_key else query

    return tx_query


From = page
"""Deprecated name - prefer 'page'

The name From overlaps with SQL parlance about selecting a table,
which is absolutely not what we're doing here. This was intended
as shorthand for 'starting from', but even that overlaps with the
concepts of 'greater than or equal' or 'less than or equal' for a
range query.

'page' makes it clearer, hopefully, that what is in view is
specifically a pagination of a previous query.
"""


def within_range(
    index: Index, *, gte: Optional[KeyAttributeType] = None, lte: Optional[KeyAttributeType] = None,
) -> QueryTransformer:
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
            query.get("ExpressionAttributeNames", dict()), **expr_attr_names
        )
        query["ExpressionAttributeValues"] = dict(
            query.get("ExpressionAttributeValues", dict()), **expr_attr_values
        )
        if key_condition_expr:
            query["KeyConditionExpression"] = (
                query.get("KeyConditionExpression", "") + key_condition_expr
            )
        return query

    return tx_query


def pipe(*funcs):
    """Left to right function composition"""

    def piped(arg):
        r = arg
        for f in funcs:
            r = f(r)
        return r

    return piped


def in_index(
    index: Index,
) -> Tuple[Callable[[KeyAttributeType], TableQuery], Callable[..., QueryTransformer]]:
    """Shorthand for calling single_partition and within_range separately"""
    return partial(single_partition, index), partial(within_range, index)
