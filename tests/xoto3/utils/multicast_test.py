import threading
import time
import typing as ty
from collections import defaultdict

from xoto3.utils.multicast import LazyMulticast


def test_lazy_multicast():
    class Recvr(ty.NamedTuple):
        nums: ty.List[int]

    CONSUMER_COUNT = 10
    NUM_NUMS = 30
    sem = threading.Semaphore(0)

    def start_numbers_stream(num_nums: int, recv):
        def stream_numbers():
            for i in range(CONSUMER_COUNT):
                sem.acquire()
                # wait for 10 consumers to start
            for i in range(num_nums):
                recv(i)

        t = threading.Thread(target=stream_numbers, daemon=True)
        t.start()
        return t.join

    mc = LazyMulticast(start_numbers_stream)  # type: ignore

    consumer_results = defaultdict(list)

    def consume_numbers():
        sem.release()
        thread_id = threading.get_ident()
        with mc(NUM_NUMS) as nums_stream:
            for i, num in enumerate(nums_stream):
                consumer_results[thread_id].append(num)
                if i == NUM_NUMS - 1:
                    break

    for i in range(CONSUMER_COUNT):
        threading.Thread(target=consume_numbers, daemon=True).start()

    time.sleep(1)

    assert len(consumer_results) == CONSUMER_COUNT

    for results in consumer_results.values():
        assert list(range(NUM_NUMS)) == results
