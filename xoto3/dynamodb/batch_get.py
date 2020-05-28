"""Utilities for BatchGets from DynamoDB"""
import typing as ty
from typing import Iterable, Tuple, Set, List
import timeit
from multiprocessing.dummy import Pool as ThreadPool
import os
from logging import getLogger

from xoto3.backoff import backoff
from xoto3.lazy_session import tlls
from xoto3.utils.iter import grouper_it, peek
from xoto3.utils.lazy import Lazy
from xoto3.dynamodb.types import TableResource

from .types import Item, KeyTuple, ItemKey, KeyAttributeType


logger = getLogger(__name__)

_BATCH_GET_CHUNKSIZE = int(os.environ.get("BATCH_GET_CHUNKSIZE", 1))
_THREADPOOL_SIZE = int(os.environ.get("BATCH_GET_THREADPOOL_SIZE", 50))
__DEFAULT_THREADPOOL: Lazy[ty.Any] = Lazy(
    lambda: ThreadPool(_THREADPOOL_SIZE) if _THREADPOOL_SIZE else None
)

_DYNAMODB_RESOURCE = tlls("resource", "dynamodb")


class KeyItemPair(ty.NamedTuple):
    key: ItemKey
    item: Item  # if empty, the key was not found


def BatchGetItem(
    table: TableResource, keys: ty.Iterable[ItemKey], **kwargs
) -> Iterable[KeyItemPair]:
    """Abstracts threading, pagination, and limited deduplication for BatchGetItem.

    A slightly lower-level interface is provided as BatchGetItemTupleKeys.

    You pass in an iterable of standard boto3 DynamoDB ItemKeys,
    e.g. `dict(id='petros')`, and get back an iterable of
    KeyItemPairs, e.g.  `KeyItemPair(key={'id': 'petros'}, val={'id':
    'petros', 'age': 88})`.

    If all you want is the non-empty (existing) items, wrap this call in the
    provided `items_only` utility.

    """
    canonical_key_attrs_order = tuple(sorted([key["AttributeName"] for key in table.key_schema]))

    def key_translator(composite_key: ItemKey) -> ty.Tuple[KeyAttributeType, ...]:
        return tuple(composite_key[key_name] for key_name in canonical_key_attrs_order)

    def key_tuple_item_pair_to_key_item_pair(ktip: KeyTupleItemPair) -> KeyItemPair:
        return KeyItemPair(
            {
                canonical_key_attrs_order[i]: ktip[0][i]
                for i in range(len(canonical_key_attrs_order))
            },
            ktip[1],
        )

    return (
        key_tuple_item_pair_to_key_item_pair(ktip)
        for ktip in BatchGetItemTupleKeys(
            table.name, (key_translator(key) for key in keys), canonical_key_attrs_order, **kwargs
        )
    )


KeyTupleItemPair = Tuple[KeyTuple, Item]


def BatchGetItemTupleKeys(
    table_name: str,
    key_value_tuples: Iterable[Tuple[KeyAttributeType, ...]],
    key_attr_names: ty.Sequence[str] = ("id",),
    *,
    dynamodb_resource=None,
    thread_pool=None,
    **kwargs,
) -> Iterable[KeyTupleItemPair]:
    """Gets multiple items from the same table in as few round trips as possible.

    The inputs are primary key tuples instead of the 'normal' item keys
    so that we can deduplicate them, as boto3 will not allow
    duplicates within a batch. This is why it is also necessary to
    supply a parallel tuple of the names of the primary key
    attributes. In many cases this will simply be ('id',), so the default is provided.

    The key names must be strings, and their order *must* match the
    order of the values in the key_value_tuples. In other words, if
    your Dynamo key is {'partition': primary_val, 'range': range_val},
    then if you supply key_names as ('range', 'partition') you must
    provide your key_value_tuples in (range_val, partition_val) order.

    Though it accepts an iterable, you should not expect multiple
    return values if you pass the same key value tuple more than
    once. Each individual batch performs coalescing of identical key
    values, as this is both required by DynamoDB and also just plain
    common sense. However, as it is not possible to coalesce key
    tuples across batches, you must also not rely on the coalescing
    behavior. If you do provide identical key value tuples and they
    get batched separately, then you will receive duplicate results.

    Items are returned as a tuple of the key value tuple and the full Dynamo Item
    as a dict. This way you can accumulate the results into a dictionary keyed
    by the first item in the tuple, and you will effectively receive all unique
    results keyed by the Tuple key you passed in.
    Missing items will simply have an empty dict as the Item, identical to the response
    you would get if you did a single GetItem call to Dynamo.

    If more than one round trip is required, either across batches of
    100 keys, or within a given batch, this handles that
    transparently.  By default will perform multiple gets in parallel
    using threads, but will not perform threaded gets if a
    dynamodb_resource is provided, since this would be unsafe.

    Also handles exponential backoff for throttling.

    """

    # the set creation does de-duplication for us
    is_empty, _, batches_of_100_iter = peek(
        (set(batch) for batch in grouper_it(100, key_value_tuples))
    )
    if is_empty:
        logger.debug("Performed 0 gets")
        # it's pretty wasteful to spin up a threadpool and start sending messages to it
        # if we have nothing to process.
        return ()
    if not dynamodb_resource and not thread_pool:
        # you didn't indicate you didn't want threads, so... here goes :)
        thread_pool = __DEFAULT_THREADPOOL()

    total_count = 0
    start_time = timeit.default_timer()

    if not dynamodb_resource and thread_pool:
        logger.debug("Sending batches to thread pool")

        def partial_get_single_batch(key_values_batch: Set[Tuple[KeyAttributeType, ...]]):
            return _get_single_batch(
                table_name,
                key_values_batch,
                key_attr_names,
                dynamodb_resource=_DYNAMODB_RESOURCE(),
                **kwargs,
            )

        # threaded implementation
        for batch in thread_pool.imap(
            partial_get_single_batch, batches_of_100_iter, _BATCH_GET_CHUNKSIZE
        ):
            for key_value_tuple, item in batch:
                total_count += 1
                yield key_value_tuple, item
    else:
        ddbr = dynamodb_resource if dynamodb_resource else _DYNAMODB_RESOURCE()
        # single-threaded serial batches
        for key_values_batch_set in batches_of_100_iter:
            results = _get_single_batch(
                table_name, key_values_batch_set, key_attr_names, dynamodb_resource=ddbr, **kwargs
            )
            for key_value_tuple, item in results:
                total_count += 1
                yield key_value_tuple, item
    ms_elapsed = (timeit.default_timer() - start_time) * 1000
    logger.info(
        "Performed %d gets in %d ms at a rate of %.1f/s",
        total_count,
        ms_elapsed,
        total_count / ms_elapsed * 1000,
    )


def _get_single_batch(
    table_name: str,
    key_values_batch: Set[Tuple[KeyAttributeType, ...]],  # up to 100
    key_attr_names: ty.Sequence[str] = ("id",),
    *,
    dynamodb_resource=None,
    **kwargs,
) -> List[KeyTupleItemPair]:
    """Does a BatchGetItem of a single batch of 100.

    Suitable for use in threaded applications, since it is non-lazy.
    """
    if not isinstance(table_name, str):
        # because we can't trust boto to give reasonable errors and I just wasted two hours
        raise ValueError(
            f"Your proposed table name {table_name} is not a string "
            "and boto3 will probably die in some mysterious way"
        )

    logger.debug("Starting up single batch get of %d on %s", len(key_values_batch), table_name)

    ddbr = dynamodb_resource if dynamodb_resource else _DYNAMODB_RESOURCE()
    batch_get_with_backoff = backoff(ddbr.batch_get_item)

    table_request = {
        "Keys": [_kv_tuple_to_key(kt, key_attr_names) for kt in key_values_batch],
        **kwargs,
    }
    output: List[KeyTupleItemPair] = list()
    while table_request and table_request.get("Keys", []):
        start = timeit.default_timer()  # solely for logging performance stats

        # perform batch get
        result = batch_get_with_backoff(RequestItems={table_name: table_request})
        responses = result.get("Responses", {}).get(table_name, [])

        # log performance
        ms_elapsed = (timeit.default_timer() - start) * 1000
        logger.debug(
            f"_get_single_batch on %s returned %d/%d items after %d ms; %.1f/s",
            table_name,
            len(responses),
            len(table_request["Keys"]),
            ms_elapsed,
            len(responses) / ms_elapsed * 1000,
        )

        # yield successful responses
        for item in responses:
            key_value_tuple = tuple(item[key] for key in key_attr_names)
            key_values_batch.remove(key_value_tuple)
            output.append((key_value_tuple, item))
        # UnprocessedKeys contains the entire original query as well as the actual unprocessed keys
        table_request = result.get("UnprocessedKeys", {}).get(table_name)

    # return empty dict for every item that does not exist in Dynamo in this batch
    if key_values_batch:
        logger.debug("Yielding %d missing items from %s", len(key_values_batch), table_name)
        for missing_item in key_values_batch:
            output.append((missing_item, dict()))
    return output


def _kv_tuple_to_key(kv_tuple, key_names):
    assert len(kv_tuple) == len(key_names)
    return {key_names[i]: kv_tuple[i] for i in range(len(key_names))}


def items_only(
    key_item_pairs: ty.Iterable[ty.Union[KeyItemPair, KeyTupleItemPair]]
) -> ty.Iterable[Item]:
    """Use with BatchGetItem if you just want the items that were
    found instead of the full iterable of all the keys you requested
    alongside their respective item or empty dict if the item wasn't
    found.

    """
    for key, item in key_item_pairs:
        if item:
            yield item
