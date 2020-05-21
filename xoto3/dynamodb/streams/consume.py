import typing as ty
import time
from uuid import uuid4
from threading import Thread
from logging import getLogger

from .shards import (
    Shard,
    key_shard,
    refresh_live_shards,
    shard_iterator_from_shard,
    yield_records_from_shard_iterator,
)

logger = getLogger(__name__)


def process_latest_from_stream(client, stream_arn: str, stream_consumer, sleep_s: int = 10):
    """This spawns a thread which spawns other threads that each handle a
    DynamoDB Stream Shard. Your consumer will get everything from every shard.

    This is therefore obviously _not_ suitable for cases where there
    might be lots and lots of data. This is a utility for other cases.

    It returns the thread itself (in case you want to block until the
    stream runs out, which incidentally will probably never happen)
    and also a thunk that may be called to poison all the existing
    shard consumers so that you don't get any more data and they
    eventually close out their threads as well.

    Note however that since Python threads are not interruptible,
    poisoning them will only take effect once they've received their
    next item from their shard. If your table is not in active use
    that may never happen.

    The good news is that all of these threads are started as daemon
    threads, so if your process exits these threads will exit
    immediately as well.

    """
    processing_id = uuid4().hex  # for debugging/sanity
    live_processors_by_key: ty.Dict[str, Thread] = dict()
    emptied_shards: ty.Set[str] = set()

    def shard_emptied(shard: Shard):
        logger.debug(f"{processing_id} shard emptied {shard}")
        shard_key = key_shard(shard)
        if shard_key in live_processors_by_key:
            live_processors_by_key.pop(shard_key)
            emptied_shards.add(shard_key)

    sentinel_container = dict(kill_me=False)

    def run():
        iterator_type = "LATEST"
        new_shards_by_key = refresh_live_shards(client, stream_arn)

        while not sentinel_container["kill_me"]:
            for key, shard in new_shards_by_key.items():
                dbg = f"{processing_id} - {key}"
                shard_iterator = shard_iterator_from_shard(client, iterator_type, shard)

                def run_shard_and_cleanup():
                    try:
                        logger.debug(
                            f'{dbg} starting shard processor with shard {shard_iterator["ShardIterator"]}'
                        )
                        for item in yield_records_from_shard_iterator(client, shard_iterator):
                            if sentinel_container["kill_me"]:
                                break
                            stream_consumer(item)
                        logger.debug(f"{dbg} exiting shard iterator processor")
                    except Exception as e:
                        logger.exception(e)
                    shard_emptied(shard)

                logger.debug(f"{dbg} spawning new shard processor for shard {key}")
                live_processors_by_key[key] = Thread(target=run_shard_and_cleanup)
                live_processors_by_key[key].start()
            iterator_type = "TRIM_HORIZON"
            # for all subsequent shards, start at their beginning

            time.sleep(sleep_s)

            new_shards_by_key = refresh_live_shards(client, stream_arn)
            for shard_key in set(live_processors_by_key.keys()) | emptied_shards:
                new_shards_by_key.pop(shard_key, None)

            if not live_processors_by_key and not new_shards_by_key:
                break  # out of the loop

    # we use a thread so that we can actually return to the caller a way to shut all this down
    # Python threads are not interruptible, so this is a little uglier than one might wish
    thread = Thread(target=run, daemon=True)
    # it's a daemon thread so that this thread will not keep your program alive by itself
    # This way, a Ctrl-C or other exit will do the trick cleanly.
    thread.start()

    def kill():
        sentinel_container["kill_me"] = True

    return thread, kill
