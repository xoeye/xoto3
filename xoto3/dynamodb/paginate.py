import typing as ty
from functools import partial
from logging import getLogger

from xoto3.paginate import yield_pages_from_operation, LastEvaluatedCallback, DYNAMODB_SCAN

from .types import Item


logger = getLogger(__name__)


dynamodb_table_yielder = partial(yield_pages_from_operation, *DYNAMODB_SCAN)


def yield_items(
    table_func, request: dict, last_evaluated_callback: LastEvaluatedCallback = None
) -> ty.Iterable[Item]:
    """Executes the given TableResource function (query or scan) on the
    given TableResource, with the request provided. The request can be
    any boto3-compatible DynamoDB request, though it is of course
    suggested that you take advantage of the provided helper methods
    in `.query` and `.conditions` to construct that request.

    If your request specifies no Limit, then this will iteratively
    yield the entire result set as you consume the results.

    If a Limit is provided in the request dict, this will iteratively
    yield only that number of items.  If you want to receive the
    LastEvaluatedKey you should provide the named callback.

    """
    for page in dynamodb_table_yielder(table_func, request, last_evaluated_callback):
        logger.debug("Retrieved a page of results from DynamoDB")
        yield from page.get("Items", [])
