# type: ignore
import queue

import pytest

from xoto3.utils.poll import QueuePollIterable, TimedOut, expiring_poll_iter


def test_queue_poll_iterable():
    qpi = QueuePollIterable(queue.Queue())
    qpi.iter_timeout = 1.0

    with pytest.raises(StopIteration):
        next(iter(qpi))

    with pytest.raises(TimedOut):
        qpi(0.5)


def test_expiring_poll_iter():
    qpi = QueuePollIterable(queue.Queue())

    epi = expiring_poll_iter(0.3)(qpi)

    with pytest.raises(StopIteration):
        next(epi)
