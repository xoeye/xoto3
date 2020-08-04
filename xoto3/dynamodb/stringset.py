from typing import Set
from copy import deepcopy
from logging import getLogger as get_logger

from .types import TableQuery
from .utils.expressions import make_unique_expr_attr_key

logger = get_logger(__name__)


def stringset_contains(
    set_attr_name: str, contains: Set[str], AND: bool = True, suffix: str = "SSCONTAINS"
):
    def tx_query(query: TableQuery) -> TableQuery:
        """Adds a set of StringSet 'contains' conditions to a DynamoDB FilterExpression.

        Takes the strings exactly the way they are, so if you want to do case-insensitive search
        the caller should take care to lowercase all the strings in 'contains' first.
        """
        query = deepcopy(query)  # non-destructive
        fex = query.get("FilterExpression", "")
        fex += " ( "

        operator = "AND" if AND else "OR"

        key = make_unique_expr_attr_key(set_attr_name + suffix)
        name = "#" + key
        value_base = ":" + key

        query["ExpressionAttributeNames"] = {
            **query.get("ExpressionAttributeNames", dict()),
            **{name: set_attr_name},
        }
        for i, string in enumerate(contains):
            value = value_base + str(i)
            fex += f"contains({name}, {value}) {operator} "
            query["ExpressionAttributeValues"] = {
                **query.get("ExpressionAttributeValues", dict()),
                **{value: string},
            }
        fex = fex[: -(1 + len(operator))]  # strip final operator

        fex += " ) "
        query["FilterExpression"] = fex
        logger.debug(f"FilterExpression: {fex}")
        return query

    return tx_query
