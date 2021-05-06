import threading
import time
import typing as ty
from logging import getLogger
from uuid import uuid4

logger = getLogger(__name__)

E = ty.TypeVar("E")
StreamEventFunnel = ty.Callable[[E], ty.Any]

Shard = ty.TypeVar("Shard")
ShardIterator = ty.TypeVar("ShardIterator")


class ShardedStreamFunnelController(ty.NamedTuple):
    thread: threading.Thread
    # the thread which will poll on a schedule for new shards and
    # activate a consumer for each new shard found.
    poison: ty.Callable[[], None]
    # call poison to stop polling for new shards and to stop each shard
    # thread when it receives its next event


def funnel_sharded_stream(
    refresh_live_shards: ty.Callable[[], ty.Dict[str, Shard]],
    startup_shard_iterator: ty.Callable[[Shard], ShardIterator],
    future_shard_iterator: ty.Callable[[Shard], ShardIterator],
    iterate_shard: ty.Callable[[ShardIterator], ty.Iterable[E]],
    # a shard should iterate until it is fully consumed.
    stream_event_funnel: StreamEventFunnel,
    shard_refresh_interval: float = 5.0,
) -> ShardedStreamFunnelController:

    """A single consumer stream processor for sharded streams.

    Spawns a thread polling for live shards, which spawns other
    threads, each of which will consume a single stream shard. Your
    event funnel will get everything from every shard. Because each
    shard will be consumed by a separate thread, your event funnel
    MUST be threadsafe.

    This is _not_ suitable for cases where there might be lots and
    lots of data on lots and lots of shards. This is a utility for
    other cases.

    Returns a thunk that may be called to poison all the existing
    shard consumers so that you don't get any more data and they
    eventually close out their threads as well, as well as the thread
    itself, which you can use to block until poisoning has happend.
    Note, however, that since Python threads are not interruptible,
    poisoning them will only take effect once they've received their
    next item from their shard. If your table is not in active use
    that may never happen.

    The good news is that all of these threads are started as daemon
    threads, so if your process exits these threads will exit
    immediately as well.

    """
    processing_id = uuid4().hex  # for debugging/sanity
    shard_processing_threads: ty.Dict[str, threading.Thread] = dict()
    emptied_shards: ty.Set[str] = set()

    def shard_emptied(shard_id: str):
        if shard_id in shard_processing_threads:
            logger.debug(f"{processing_id} shard emptied {shard_id}")
            shard_processing_threads.pop(shard_id, None)
            emptied_shards.add(shard_id)

    cloth = dict(poisoned=False)
    # ...many threads

    initial_fetch_completed = threading.Semaphore(0)
    # we want to fetch the live shards and their iterators before this
    # function so that client code that might generate new shards (by,
    # for instance, writing a new item) cannot run before we're
    # 'ready' to identify those shards as new and start processing
    # them from the beginning.
    #
    # In other words, this is how we make sure that anything done by a
    # user of this method after calling this method will show up in
    # their consumer.

    def find_and_consume_all_shards():
        logger.info(f"{processing_id} Beginning to find and consume all shards")
        shard_iterator_fetcher = startup_shard_iterator
        shards_not_yet_started = refresh_live_shards()

        while not cloth["poisoned"]:
            for shard_id, shard in shards_not_yet_started.items():
                dbg = f"{processing_id} - {shard_id}"
                shard_iterator = shard_iterator_fetcher(shard)

                def consume_shard():
                    try:
                        logger.info(f"{dbg} starting shard processor with shard {shard_iterator}")
                        for event in iterate_shard(shard_iterator):
                            if cloth["poisoned"]:
                                break
                            stream_event_funnel(event)
                        logger.info(f"{dbg} exiting shard iterator processor")
                    except Exception as e:
                        logger.exception(e)
                    shard_emptied(shard_id)

                logger.info(f"{dbg} spawning new shard processor for shard {shard_id}")
                shard_processing_threads[shard_id] = threading.Thread(
                    target=consume_shard, daemon=True
                )
                shard_processing_threads[shard_id].start()

            initial_fetch_completed.release()
            # the fact that this gets released more than once is not a problem.
            # we only use it to guard the first trip through the for loop above.

            # we've released the main thread. any future shards that
            # we discover may use a different strategy for choosing
            # what part of the shard to start at.
            shard_iterator_fetcher = future_shard_iterator

            time.sleep(shard_refresh_interval)

            shards_not_yet_started = refresh_live_shards()
            logger.info(f"{processing_id} refreshing live shards")
            for shard_id in set(shard_processing_threads) | emptied_shards:
                shards_not_yet_started.pop(shard_id, None)  # type: ignore

        logger.info(f"{processing_id} Ending search for shards")

    # we use a thread so that we can actually return to the caller a way to shut all this down
    # Python threads are not interruptible, so this is a little uglier than one might wish
    thread = threading.Thread(target=find_and_consume_all_shards, daemon=True)
    # it's a daemon thread so that this thread will not keep your program alive by itself
    # This way, a Ctrl-C or other exit will do the trick cleanly.
    thread.start()

    initial_fetch_completed.acquire()
    # once the initial fetch has completed, we can let the caller know
    # that it's safe to proceed, in case they want to write anything to
    # the table.

    def poison():
        logger.info(f"{processing_id} poisoning shard runners")
        cloth["poisoned"] = True

    return ShardedStreamFunnelController(thread, poison)
