import contextlib
import queue
import threading
import typing as ty
from functools import partial

from .poll import QueuePollIterable


class _Producer(ty.NamedTuple):
    queues: ty.Dict[int, queue.Queue]
    cleanup: ty.Callable[[], ty.Any]


E = ty.TypeVar("E")

H = ty.TypeVar("H", bound=ty.Hashable)

OnNext = ty.Callable[[E], ty.Any]
Cleanup = ty.Callable[[], ty.Any]


class LazyMulticast(ty.Generic[H, E]):
    """Allows concurrent process-local subscribers to an expensive
    producer.  Each subscriber will receive _every_ event produced after
    their subscription begins.

    Almost by definition, your producer should exist in a thread.
    `start_producer` should return only after the producer is fully
    set up, but should not block beyond what is necessary to get the
    producer running.

    Implemented as a factory for ContextManagers, to allow clients to
    easily unsubscribe.

    Expects its subscribers to operate within threads, so it is
    threadsafe.

    Because this anticipates the use of threads, it also allows
    subscribers to use an interface that may optionally be
    non-blocking. See QueuePollIterable for details.
    """

    def __init__(
        self, start_producer: ty.Callable[[H, ty.Callable[[E], ty.Any]], Cleanup],
    ):
        self.lock = threading.Lock()
        self.start_producer = start_producer
        self.producers: ty.Dict[H, _Producer] = dict()

    def _recv_event_from_producer(self, producer_key: H, producer_event: E):
        ss = self.producers.get(producer_key)
        if not ss:
            # no current consumers
            return
        for q in list(ss.queues.values()):
            q.put(producer_event)

    def __call__(self, producer_key: H) -> ty.ContextManager[QueuePollIterable[E]]:
        """Constructs a context manager that will provide access to the
        underlying multicasted producer.

        This context manager is inactive until entered using `with` -
        i.e no producer exists or is subscribed.
        """

        @contextlib.contextmanager
        def queue_poll_context() -> ty.Iterator[QueuePollIterable[E]]:
            with self.lock:
                if producer_key not in self.producers:
                    # create a single shared producer
                    cleanup = self.start_producer(
                        producer_key, partial(self._recv_event_from_producer, producer_key)
                    )
                    self.producers[producer_key] = _Producer(dict(), cleanup)

                ss = self.producers[producer_key]
                q: queue.Queue = queue.Queue()
                ss.queues[id(q)] = q

            yield QueuePollIterable(q)

            with self.lock:
                # clean up the consumer
                ss.queues.pop(id(q))
                if not ss.queues:
                    # remove the producer consumer if no one is listening
                    ss.cleanup()
                    self.producers.pop(producer_key)

        return queue_poll_context()
