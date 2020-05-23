"""A module that provides the core functionality for being able to
write async code that transparently batches requests to Dynamo.
"""
import typing as ty
from collections import defaultdict
from timeit import default_timer
import traceback
import logging

import asyncio
from asyncio import Queue, get_event_loop

from xoto3.lazy_session import tlls
from .batch_get import BatchGetItemTupleKeys


logger = logging.getLogger(__name__)


DYNAMODB_RESOURCE = tlls("resource", "dynamodb")


# When running in a Lambda, the round-trip latency to Dynamo is
# 10-20ms (in a 1024MB Lambda).  Thus, waiting for up to 10ms before
# dispatching any request, as long as this means at least one more
# request arrives during the window to be batched, is more efficient
# already than executing both serially. In most use cases we expect
# far more than 2 requests to arrive during the window, leading to far
# greater speedup. And even in the case where only one request
# arrives, we are still only slowing ourselves by a factor of < 2, so
# there is not a significant penalty even in the worst case.
_DEFAULT_BATCHING_WINDOW_SECONDS = 0.005
_DEFAULT_MAX_BATCH_SIZE: int = 100


class AsyncBatchRequestFulfiller(ty.NamedTuple):
    queue: asyncio.Queue
    request_map: ty.Dict[asyncio.Future, ty.Any]
    async_task: asyncio.Task


__event_loop_request_fulfillers: ty.Dict[
    ty.Any, ty.Dict[str, AsyncBatchRequestFulfiller]
] = defaultdict(dict)


def _batch_get_processor(
    table_name: str,
    item_key_tuples: ty.List[tuple],
    key_attr_names: ty.Sequence[str] = ("id",),
    dynamo_db_resource=None,
) -> ty.List[dict]:
    resp = BatchGetItemTupleKeys(
        table_name, item_key_tuples, key_attr_names, dynamodb_resource=dynamo_db_resource
    )
    indexed_by_kt = {res[0]: res[1] for res in resp}
    return [indexed_by_kt[key_tuple] for key_tuple in item_key_tuples]


async def queue_batching_fulfiller(
    batch_processor: ty.Callable[[list], list],
    queue: asyncio.Queue,
    future_to_request_map: ty.Dict[asyncio.Future, ty.Any],
    batch_wait_s: float = _DEFAULT_BATCHING_WINDOW_SECONDS,
    max_batch_size: int = _DEFAULT_MAX_BATCH_SIZE,
    logging_name: str = "",
):
    """This is the long-running async batching loop that takes Future
    requests over a Queue, batches them together, and sends them to a
    batch processor.
    """
    if not logging_name:
        logging_name = str(batch_processor)
    try:
        logger.debug(
            f"Entering new batching loop for {logging_name} with key attributes "
            f"batching window {batch_wait_s} seconds"
        )
        while True:
            # wait for at least one request
            logger.debug(f"Indefinitely awaiting the next queued request for {logging_name}")
            request_futures = [await queue.get()]
            logger.debug(f"Received a queued request for {logging_name}")
            start_time = default_timer()

            # wait for as many requests as we can, up to the batch request limit, within a small batching window
            try:
                while len(request_futures) < max_batch_size:
                    time_left = batch_wait_s - (default_timer() - start_time)
                    logger.debug(
                        f"Waiting for queue for {time_left} seconds in order to process a larger batch."
                    )
                    request_futures.append(await asyncio.wait_for(queue.get(), time_left))
            except asyncio.TimeoutError:
                pass

            logger.debug(f"Received {len(request_futures)} requests before closing the batch.")

            logger.debug("Starting batch processor!")
            results = batch_processor([future_to_request_map.pop(fut) for fut in request_futures])
            logger.debug("Finished batch processor!")
            # we should always have received a result for every request we made...
            assert len(results) == len(request_futures)

            for i in range(len(request_futures)):
                fut = request_futures[i]
                result = results[i]
                fut.set_result(result)

            logger.debug(f"Finshed setting all {len(request_futures)} future results")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        traceback.print_exc()
        raise e  # this is for easier debugging inside async tasks
    finally:
        logger.info(f"Exiting the Dynamo BatchGet task for {logging_name}")


def spawn_dynamo_fulfiller(
    table_name: str, key_attr_names: ty.Sequence[str]
) -> AsyncBatchRequestFulfiller:
    """Creates a new AsyncBatchRequestFulfiller for the given table name.

    Each fulfiller has its own queue and its own batching loop, so
    requests made to a given queue will be fulfilled by that loop even
    if another one exists in the same program or even the same event
    loop.

    """
    logger.info(
        f"Spawning new dynamo fulfiller for {table_name} with key attributes {key_attr_names}"
    )
    queue: asyncio.Queue = Queue(100)

    resource = DYNAMODB_RESOURCE()

    def table_batch_get_fulfiller(key_tuples: ty.List[tuple]) -> ty.List[dict]:
        return _batch_get_processor(table_name, key_tuples, key_attr_names, resource)

    request_map: ty.Dict[asyncio.Future, ty.Any] = dict()
    task = asyncio.create_task(
        queue_batching_fulfiller(
            table_batch_get_fulfiller, queue, request_map, logging_name=table_name
        )
    )
    fulfiller = AsyncBatchRequestFulfiller(  # type: ignore
        queue, request_map, task
    )
    return fulfiller


def ensure_table_request_fulfiller(
    table_name: str, key_attr_names: ty.Sequence[str]
) -> AsyncBatchRequestFulfiller:
    """Returns an AsyncBatchRequestFulfiller for the active event loop and the given table name,
    or creates one if it does not yet exist."""
    our_loop = get_event_loop()
    our_event_loop_fulfillers = __event_loop_request_fulfillers[our_loop]
    if table_name not in our_event_loop_fulfillers:
        our_event_loop_fulfillers[table_name] = spawn_dynamo_fulfiller(table_name, key_attr_names)
    return our_event_loop_fulfillers[table_name]


def cancel_all_dynamo_fulfillers():
    for _loop, fulfiller_dict in __event_loop_request_fulfillers.items():
        for _table_name, fulfiller in fulfiller_dict.items():
            fulfiller.async_task.cancel()


async def get_item(
    table_name: str, primary_key_tuple: tuple, primary_key_attr_names: ty.Sequence[str] = ("id",)
) -> dict:
    """Performs Dynamo request batching behind the scenes.

    The primary key tuple must be in the order defined by the primary_key_attr_names.

    This is guaranteed to be slower than just asking for a single item
    directly, because the batcher waits a short period to maximize the
    size of its batches, so only use this if you know your client code
    is going to be making multiple async requests.

    You should almost certainly wrap this function with a helper
    method for your given table and item type.

    """
    logger.debug(f"Asking for {primary_key_tuple}")
    fulfiller = ensure_table_request_fulfiller(table_name, primary_key_attr_names)
    fut = get_event_loop().create_future()
    fulfiller.request_map[fut] = primary_key_tuple
    await fulfiller.queue.put(fut)
    return await fut
