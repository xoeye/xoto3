import typing as ty
from copy import deepcopy
from functools import partial

# dear goodness AWS's pagination utilities are bare-bones...
# here's a bit of general purpose logic that can probably be reused lots of places...

KeyPath = ty.Tuple[str, ...]
LastEvaluatedCallback = ty.Optional[ty.Callable[[ty.Any], ty.Any]]


def get_at_path(path, d):
    for path_elem in path:
        d = d.get(path_elem, None)
        if d is None:
            return d
    return d


def set_at_path(path, d, val):
    for path_elem in path[:-1]:
        d = d[path_elem]
    d[path[-1]] = val


def yield_pages_from_operation(
    exclusive_start_path: KeyPath,
    last_evaluated_path: KeyPath,
    limit_path: ty.Tuple[str, ...],
    items_path: ty.Tuple[str, ...],
    # whether or not limiting _happens_ is controlled by whether you set a limit in your request dict
    # but if you provide limit_path you must provide items_path and vice-versa,
    # or we won't be able figure out how to create the new limit for each paged request.
    operation: ty.Callable[..., dict],
    # the thing that turns a request into the next page of a response
    request: dict,
    # your basic request
    last_evaluated_callback: LastEvaluatedCallback = None,
) -> ty.Iterable[dict]:
    """Look, here's the deal...

    boto3 (and AWS APIs in general) have a fairly consistent
    pagination behavior for requests which can/must be paginated.

    These requests are usually called 'methods', 'operations', or
    'actions' in the boto3 documentation. Our unified term is
    'operation'.

    You perform an operation with a request that tells them that you
    want to start 'at the beginning', effectively doing so by not
    supplying what is usually called something like
    ExclusiveStart[Key]. The operation gives you back a page of
    results, and it also gives you back something that is sort of like
    a bookmark - it says where you left off in your pagination.

    If you want the next page of results, you pass your original request
    back plus that 'bookmark' as the ExclusiveStartThingy.

    Each time, they'll pass you back a new bookmark, until you finally
    get the last page of results, at which point they'll pass you back
    an 'empty' bookmark. When they do that, you know there are no more pages.

    Most of these same API operations also support a related behavior
    called 'limiting', which allows you to request that your dataset
    'end' after N items are found. The reason this behavior is built
    in here (instead of having a separate abstraction) is that the
    'bookmark' is usually an opaque token based on the very last item
    returned and therefore cannot itself be 'adjusted' by a specific
    number of items. Once you've received a bookmark, there's no way
    of 'resuming' your pagination from a point partway through a
    page. If your end client needs to receive data in concrete page
    sizes, then the only way to support that without requiring
    something other than the end client to maintain pagination state
    is to pass that limit request all the way to the underlying
    system.

    This is an attempt to build a general-purpose abstraction for
    those two API behaviors.

    Note that this function does _not_ paginate the items within each
    page for you. It returns the entire, unchanged response from each
    time it calls the operation. The items themselves will be
    contained within that page and you can process them and their
    metadata as you please.

    You _probably_ want to construct a partially-applied function
    containing the first 4 arguments (which define the behavior for a
    specific operation), so that you can then invoke the same
    operation paginator repeatedly with different requests.

    """
    assert all((limit_path, items_path)) or not any((limit_path, items_path))
    request = deepcopy(request)
    # we make a copy of your request because we're going to modify it
    # as we paginate but you shouldn't have to deal with that.

    get_le = partial(get_at_path, last_evaluated_path)
    set_es = partial(set_at_path, exclusive_start_path)
    get_limit = partial(get_at_path, limit_path)
    set_limit = partial(set_at_path, limit_path)
    get_items = partial(get_at_path, items_path)

    # the limiting logic is an add-on and does not have to be used
    starting_limit = 0
    if limit_path:
        assert items_path
        starting_limit = get_limit(request)

    limit = starting_limit
    ExclusiveStart: ty.Any = get_le(request) or ""

    while ExclusiveStart is not None:
        assert limit is None or limit >= 0
        if ExclusiveStart:
            set_es(request, ExclusiveStart)
        if limit:
            set_limit(request, limit)
        page_response = operation(**request)
        yield page_response  # we yield the entire response
        ExclusiveStart = get_le(page_response) or None
        if starting_limit:
            # a limit was requested
            limit = limit - len(get_items(page_response))
            if limit <= 0:
                # we're done; before we leave, provide last evaluated if requested
                if last_evaluated_callback:
                    last_evaluated_callback(ExclusiveStart)
                ExclusiveStart = None


# below are some arguments that can be used to paginate existing methods given as useful examples

DYNAMODB_SCAN = (("ExclusiveStartKey",), ("LastEvaluatedKey",), ("Limit",), ("Items",))
DYNAMODB_QUERY = DYNAMODB_SCAN  # these are the same
# e.g. partial(yield_pages_from_request, *DYNAMODB_QUERY)(table.query, your_query_request)

DYNAMODB_STREAMS_DESCRIBE_STREAM = (
    ("ExclusiveStartShardId",),
    ("StreamDescription", "LastEvaluatedShardId"),
    ("Limit",),
    ("StreamDescription", "Shards"),
)

DYNAMODB_STREAMS_GET_RECORDS = (
    ("ShardIterator",),
    ("NextShardIterator",),
    ("Limit",),
    ("Records",),
)

S3_LIST_OBJECTS_V2 = (
    ("ContinuationToken",),
    ("NextContinuationToken",),
    ("MaxKeys",),
    ("Contents",),
)
