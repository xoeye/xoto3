import typing as ty
from functools import partial
from logging import getLogger

from xoto3.paginate import yield_pages_from_operation, LastEvaluatedCallback, DYNAMODB_SCAN

from .types import Item


logger = getLogger(__name__)


dynamo_table_yielder = partial(yield_pages_from_operation, *DYNAMODB_SCAN)


def yield_dynamo_items(
    table_func, request: dict, last_evaluated_callback: LastEvaluatedCallback = None
) -> ty.Iterable[Item]:
    """Continues to iterate across table and yields all items even through pages.

    If a Limit is provided in the request dict, this function honors that.
    If you want to receive the LastEvaluatedKey you should provide a callback.

    Works for `scan` and `query`.
    """
    for page in dynamo_table_yielder(table_func, request, last_evaluated_callback):
        logger.debug("Retrieved a page of results from DynamoDB")
        yield from page.get("Items", [])
