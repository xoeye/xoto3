from typing import Tuple, Hashable, Callable, Any, Mapping, Set, Union, cast
from functools import singledispatch, wraps
import inspect


SimpleTransform = Callable[[Any], Any]

PathTransformReturn = Tuple[Any, bool]
# a path transform takes the object to be transformed and the key path
# to it, and returns a bool for whether or not to stop recursing down
# the tree and the transformed object
KeyPath = Tuple[Hashable, ...]
PathTransform = Callable[[Any, KeyPath], Tuple[Any, bool]]

TreeTransform = Union[SimpleTransform, PathTransform]


def pathed_from_simple(simple_tx: SimpleTransform) -> PathTransform:
    @wraps(simple_tx)
    def pathed_tx(obj: Any, _path: KeyPath) -> Tuple[Any, bool]:
        return simple_tx(obj), False  # never 'stop' for a simple transform

    return pathed_tx


def coerce_transform(transform: TreeTransform) -> PathTransform:
    try:
        is_simple_transform = len(inspect.signature(transform).parameters) == 1
    except ValueError:
        is_simple_transform = True  # likely a builtin that cannot be introspected
    return (
        pathed_from_simple(cast(SimpleTransform, transform))
        if is_simple_transform
        else cast(PathTransform, transform)
    )


def map_tree(transform: TreeTransform, obj: Any) -> Any:
    """Maps a tree made of Python general-purpose builtin containers.

    The tree property of the object is important - technically you may
    submit a DAG, but the children will get transformed P times where
    P is the number of unique paths in the graph leading to them from
    the root. It is critical, of course, that you not provide an
    object with a cycle in its graph - this will cause an infinite
    cycle.

    Does a depth-first walk of the object, calling the transform as a
    preorder operation before then recursing into mappings, lists,
    tuples, and sets, rendering a new corresponding builtin instance
    for each.

    Only applies the first recursive transform that matches the type
    of the provided object.

    Does not preserve subtypes of Set or Mapping (you get builtin sets
    and dicts).

    Does not natively support iterables that are not tuples or lists,
    because (for instance) consuming generators is likely to lead to
    very unexpected behavior, as it effectively is a side-effect
    rather than a pure transformation. If you wish to, for instance,
    recurse into generators, your transform can return a reified list
    or tuple.

    There are two options for your transform.

    If you provide a SimpleTransform (a callable taking a single
    parameter), your transform will be called as such, and you will
    have no access to path information.

    If you want 'access' to the current path in your transform, you
    can provide a PathTransform, which will be called with both the
    object and its "KeyPath", and your transform must return the
    transformed object and a boolean indicating whether to stop the
    tree walk (recursion) for this branch of the tree.

    Note that the KeyPath is comprised only of keys in mappings -
    lists, tuples, and sets are recursed through without adding any
    'index' information to the path. This is because there cannot be a
    meaningful 'index' operation on sets, and therefore a path would
    be 'broken' no matter what at the point in the graph where a set
    was present. Rather than try to provide different behavior for
    data structures that don't support the concept of indexing, we're
    providing a reduced but logical subset of behavior where 'named'
    paths only are provided.

    """
    return _map_tree(coerce_transform(transform), obj)


def _map_tree(transform: PathTransform, obj: Any, *, path: KeyPath = ()) -> Any:
    obj, stop = transform(obj, path)
    if stop:
        return obj

    # then apply the first builtin-type-matching recursive transform.
    if isinstance(obj, Mapping):
        return {k: _map_tree(transform, v, path=path + (k,)) for k, v in obj.items()}
    if isinstance(obj, Set):
        return {_map_tree(transform, member, path=path) for member in obj}
    if isinstance(obj, list):
        return [_map_tree(transform, item, path=path) for item in obj]
    if isinstance(obj, tuple):
        return tuple((_map_tree(transform, item, path=path) for item in obj))

    return obj


def _tuple_starts_with(a: tuple, b: tuple) -> bool:
    for i, b_i in enumerate(b):
        try:
            if b_i != a[i]:
                return False
        except IndexError:
            return False
    return True


def make_path_stop_transform(target_path: KeyPath, transform: SimpleTransform) -> PathTransform:
    target_path = tuple(target_path)  # in case it's a list or something

    def path_tx(item: Any, path: KeyPath) -> Tuple[Any, bool]:
        stop = not _tuple_starts_with(target_path, path) or path == target_path
        # stop if not part of target path, or if we've reached the full path
        if path == target_path:
            return transform(item), stop
        return item, stop

    return path_tx


def make_path_only_transform(target_path: KeyPath, transform: SimpleTransform) -> PathTransform:
    target_path = tuple(target_path)  # in case it's a list or something

    def path_only_tx(item: Any, path: KeyPath) -> Tuple[Any, bool]:
        if path == target_path:
            return transform(item), False
        return item, False

    return path_only_tx


def type_dispatched_transform(tx_map: Mapping[type, TreeTransform]) -> PathTransform:
    @singledispatch
    def tx_type(obj: Any, _path: KeyPath) -> PathTransformReturn:
        # passthrough base case
        return obj, False

    for Type, transform in tx_map.items():
        tx_type.register(Type, coerce_transform(transform))
    return tx_type


def compose(*txs: TreeTransform) -> TreeTransform:
    """Right to left TreeTransform composition"""
    ptxs = list(reversed([coerce_transform(tx) for tx in txs]))

    def composed(item: Any, path: KeyPath) -> PathTransformReturn:
        stop = False
        for ptx in ptxs:
            item, stop = ptx(item, path)
            if stop:
                break
        return item, stop

    return composed
