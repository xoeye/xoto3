import contextlib
from typing import Callable, ContextManager, Iterator, TypeVar

X = TypeVar("X")
Y = TypeVar("Y")


def xf_cm(xf: Callable[[X], Y]) -> Callable[[ContextManager[X]], ContextManager[Y]]:
    """Transform a ContextManager that returns X into a ContextManager that returns Y.

    By 'returns' we mean the value returned by __enter__.

    Useful if you commonly want to use a particular type of context manager in a different way.
    """

    def _(cm: ContextManager[X]) -> ContextManager[Y]:
        @contextlib.contextmanager
        def xfing_context() -> Iterator[Y]:
            with cm as entered_cm:
                yield xf(entered_cm)

        return xfing_context()

    return _
