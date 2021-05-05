"""A thin but not terribly composable interface combining the worlds
of "things you might want to iterate over" and "things that you might
not want to block your thread on indefinitely.
"""
import queue
import time
from typing import Callable, Iterable, Iterator, Optional, TypeVar

from typing_extensions import Protocol

X = TypeVar("X")
Y_co = TypeVar("Y_co", covariant=True)


class TimedOut(Exception):
    pass


class Poll(Protocol[Y_co]):
    def __call__(self, __timeout: Optional[float] = None) -> Y_co:
        """raises TimedOut exception after timeout"""
        ...  # pragma: nocover


def expiring_poll_iter(seconds_from_start: float) -> Callable[[Poll[X]], Iterator[X]]:
    """Stops iterating (does not raise) when time has expired."""

    def _expiring_iter(poll: Poll[X]) -> Iterator[X]:
        time_left = seconds_from_start
        end = time.monotonic() + seconds_from_start
        while time_left > 0:
            try:
                yield poll(time_left)
            except TimedOut:
                pass
            time_left = end - time.monotonic()

    return _expiring_iter


class QueuePollIterable(Iterable[X], Poll[X]):
    """A convenience implementation that provides infinite queue iterators
    to simple clients, and a simplified polling interface to clients
    that have a need to control timeout behavior.

    """

    def __init__(self, q: queue.Queue):
        self.q = q
        self.iter_timeout = None

    def __iter__(self) -> Iterator[X]:
        while True:
            try:
                yield self(self.iter_timeout)
            except TimedOut:
                return

    def __call__(self, timeout: Optional[float] = None) -> X:
        """Raises TimedOut after timeout"""
        try:
            return self.q.get(block=True, timeout=timeout)
        except queue.Empty:
            raise TimedOut()
