from typing import Set
from copy import deepcopy
from logging import getLogger as get_logger

from .types import TableQuery

logger = get_logger(__name__)


def stringset_contains(set_attr_name: str, contains: Set[str], AND=True):
    def tx_query(query: TableQuery) -> TableQuery:
        """Adds a set of StringSet 'contains' conditions to a DynamoDB FilterExpression.

        Takes the strings exactly the way they are, so if you want to do case-insensitive search
        the caller should take care to lowercase all the strings in 'contains' first.
        """
        query = deepcopy(query)  # non-destructive
        fex = query.get("FilterExpression", "")
        fex += " ( "

        operator = "AND" if AND else "OR"

        query["ExpressionAttributeNames"] = {
            **query["ExpressionAttributeNames"],
            **{f"#{set_attr_name}": set_attr_name},
        }
        for i, string in enumerate(contains):
            fex += f" contains(#{set_attr_name}, :{set_attr_name}{i}) {operator} "
            query["ExpressionAttributeValues"] = {
                **query["ExpressionAttributeValues"],
                **{f":{set_attr_name}{i}": string},
            }
        fex = fex[: -(1 + len(operator))]  # strip final operator

        fex += " ) "
        query["FilterExpression"] = fex
        logger.debug(f"FilterExpression: {fex}")
        return query

    return tx_query
