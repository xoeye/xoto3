import typing as ty
import itertools


def get_n_at_a_time(l: list, n: int) -> ty.Iterable[list]:
    """Prefer grouper_it."""
    for i in range(0, len(l), n):
        yield l[i : i + n]


def grouper_it(n: int, iterable: ty.Iterable) -> ty.Iterable[ty.Iterable]:
    """Iterative version of get_n_at_a_time"""
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)


def strip_nones(d: dict) -> dict:
    """Removes keys where the values are None"""
    return {key: val for key, val in d.items() if val is not None}
