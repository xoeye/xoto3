import typing as ty

import boto3

from xoto3.stream import (
    ShardedStreamFunnelController,
    StreamEventFunnel,
    StreamFunnelMulticast,
    funnel_sharded_stream,
)

from .records import ItemImages, old_and_new_items_from_stream_event_record
from .shards import (
    refresh_live_shards,
    shard_iterator_from_shard,
    yield_records_from_shard_iterator,
)

DynamoDbStreamEventConsumer = ty.Callable[[dict], None]


def process_latest_from_stream(
    client, stream_arn: str, stream_consumer: DynamoDbStreamEventConsumer, sleep_s: float = 10.0
) -> ShardedStreamFunnelController:
    """This spawns a thread which spawns other threads that each handle a
    DynamoDB Stream Shard. Your consumer/funnel will get everything from every shard.

    See the docstring on funnel_sharded_stream for further details.
    """
    return funnel_sharded_stream(
        lambda: refresh_live_shards(client, stream_arn),
        lambda shard: shard_iterator_from_shard(client, "LATEST", shard),
        lambda shard: shard_iterator_from_shard(client, "TRIM_HORIZON", shard),
        lambda shard_it: yield_records_from_shard_iterator(client, shard_it),
        stream_consumer,
        shard_refresh_interval=sleep_s,
    )


def make_dynamodb_stream_images_multicast(
    shard_refresh_interval: float = 2.0,
) -> StreamFunnelMulticast[ItemImages]:
    """A slightly cleaner interface to process_latest_from_stream,
    particularly if you want to be able to share the output across
    multiple consumers.
    """

    def start_dynamodb_stream(
        table_name: str, stream_event_callback: StreamEventFunnel[ItemImages]
    ) -> ty.Callable[[], ty.Any]:
        session = boto3.session.Session()
        table = session.resource("dynamodb").Table(table_name)
        thread, kill = process_latest_from_stream(
            session.client("dynamodbstreams"),
            table.latest_stream_arn,  # type: ignore
            lambda record_dict: stream_event_callback(
                old_and_new_items_from_stream_event_record(record_dict)
            ),
            sleep_s=shard_refresh_interval,
        )

        def cleanup_ddb_stream():
            kill()
            thread.join()

        return cleanup_ddb_stream

    return StreamFunnelMulticast(start_dynamodb_stream)
