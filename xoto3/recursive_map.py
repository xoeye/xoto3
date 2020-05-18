from typing import Tuple, Hashable, Callable, Any, Mapping, Set, Optional, Sequence


KeyPath = Tuple[Hashable, ...]
SimpleTransform = Callable[[Any], Any]
PathCallback = Callable[[KeyPath, Any], bool]


def map_recursively(
        transform: SimpleTransform,
        obj: Any,
        *,
        path: tuple = (),
        path_callback: Optional[PathCallback] = None
) -> Any:
    """Does a depth-first walk of the object, calling the transform as a
    preorder operation before then recursing into mappings (dicts),
    lists, tuples, and sets, rendering a new corresponding builtin
    instance for each.

    Only applies the first recursive transform that matches the type
    of the provided object.

    Does not preserve subtypes of Set or Mapping.

    Does not natively support iterables that are not tuples or lists,
    because (for instance) consuming generators is likely to lead to
    very unexpected behavior, as it effectively is a side-effect
    rather than a pure transformation. If you wish to, for instance,
    recurse into generators, your transform can return a reified list
    or tuple.

    If you want 'access' to the current path in your transform, you
    can provide an additional path_callback which will receive the
    path immediately _prior_ to each call to your transform, and which
    returns True if you wish the recursion to short-circuit
    immediately _after_ the transform.
    """
    stop = False
    if path_callback:
        stop = path_callback(path, obj)
    obj = transform(obj)
    if stop:
        return obj

    # then apply the first builtin-type-matching recursive transform.
    if isinstance(obj, Mapping):
        return {
            k: map_recursively(transform, v, path=path + (k,), path_callback=path_callback)
            for k, v in obj.items()
        }
    if isinstance(obj, Set):
        return {
            map_recursively(transform, member, path=path, path_callback=path_callback)
            for member in obj
        }
    if isinstance(obj, list):
        return [
            map_recursively(transform, item, path=path, path_callback=path_callback)
            for item in obj
        ]
    if isinstance(obj, tuple):
        return tuple((
            map_recursively(transform, item, path=path, path_callback=path_callback)
            for item in obj
        ))

    return obj


def tuple_starts_with(a: tuple, b: tuple) -> bool:
    for i, b_i in enumerate(b):
        try:
            if b_i != a[i]:
                return False
        except IndexError:
            return False
    return True



class PathedTransform:
    """For use with map_recursively"""

    def __init__(self, transform: SimpleTransform, target_path: Sequence[Hashable]):
        self.transform = transform
        self.target_path = target_path

    def path_callback(self, path: Sequence[Hashable], *_args):
        self.current_path = path
        # stop if not part of target path, or if we've reached the full path
        return not tuple_starts_with(self.target_path, path) or self.current_path == self.target_path

    def __call__(self, obj: Any) -> Any:
        if self.current_path == self.target_path:
            return self.transform(obj)
        return obj
