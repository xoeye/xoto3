from typing import Set
from logging import getLogger as get_logger

logger = get_logger(__name__)


def add_stringset_contains_to_query(
    query: dict, set_attr_name: str, contains: Set[str], AND=True
) -> dict:
    """Adds a set of StringSet 'contains' conditions to a DynamoDB FilterExpression.

    Takes the strings exactly the way they are, so if you want to do case-insensitive search
    the caller should take care to lowercase all the strings in 'contains' first."""
    query = {**query}  # non-destructive
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
