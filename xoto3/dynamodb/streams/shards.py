import typing as ty
from typing_extensions import TypedDict

import boto3

from xoto3.paginate import (
    yield_pages_from_operation,
    DYNAMODB_STREAMS_DESCRIBE_STREAM,
    DYNAMODB_STREAMS_GET_RECORDS,
)


class _SequenceNumberRange(TypedDict):
    StartingSequenceNumber: str


class SequenceNumberRange(_SequenceNumberRange, total=False):
    EndingSequenceNumber: str


class Shard(TypedDict):
    StreamArn: str
    ShardId: str
    ParentShardId: str
    SequenceNumberRange: SequenceNumberRange


class ShardIterator(TypedDict):
    ShardIterator: str


class StreamsClient:
    def __init__(self):
        self.client = boto3.client("dynamodbstreams")

    def yield_shards(self, StreamArn: str) -> ty.Iterable[Shard]:
        """This is an expensive operation relatively speaking

        The API limits are 10 calls per second across all clients, so it should be used sparingly.
        """
        return yield_shards(self.client, StreamArn)

    def yield_records_from_shard(self, ShardIteratorType: str, shard: Shard) -> ty.Iterable[dict]:
        return yield_records_from_shard_iterator(
            self.client, shard_iterator_from_shard(self.client, ShardIteratorType, shard)
        )

    def __getattr__(self, name):
        return getattr(self.client, name)

    def __dir__(self):
        """For delegating autocompletion"""
        return (
            list(super().__dir__()) + [str(key) for key in self.__dict__] + list(dir(self.client))
        )


def get_stream_arn_for_table(table_name: str, streams) -> str:
    return list(filter(lambda s: s["TableName"] == table_name, streams))[0]["StreamArn"]


def yield_shards(client, StreamArn: str) -> ty.Iterable[Shard]:
    page_yielder = yield_pages_from_operation(
        *DYNAMODB_STREAMS_DESCRIBE_STREAM, client.describe_stream, dict(StreamArn=StreamArn)
    )
    for page in page_yielder:
        for shard in page["StreamDescription"]["Shards"]:
            yield ty.cast(Shard, dict(shard, StreamArn=StreamArn))


def shard_iterator_from_shard(
    client, ShardIteratorType: str, shard: Shard, **kwargs
) -> ShardIterator:
    return client.get_shard_iterator(
        StreamArn=shard["StreamArn"],
        ShardId=shard["ShardId"],
        ShardIteratorType=ShardIteratorType,
        **kwargs,
    )


def yield_records_from_shard_iterator(client, shard_iterator: ShardIterator) -> ty.Iterable[dict]:
    yielder = yield_pages_from_operation(
        *DYNAMODB_STREAMS_GET_RECORDS,
        client.get_records,
        dict(ShardIterator=shard_iterator["ShardIterator"]),
    )
    for page in yielder:
        yield from page.get("Records", [])


def is_shard_live(shard: Shard) -> bool:
    return "EndingSequenceNumber" not in shard["SequenceNumberRange"]


def only_live_shards(shards: ty.Iterable[Shard]) -> ty.Iterable[Shard]:
    for shard in shards:
        if is_shard_live(shard):
            yield shard


def key_shard(shard: Shard) -> str:
    return shard["ShardId"]


def key_shards(shards: ty.List[Shard]) -> ty.Dict[str, Shard]:
    return {key_shard(shard): shard for shard in shards}


def live_shard_chains(shards: ty.List[Shard]) -> ty.Iterable[ty.List[Shard]]:
    shards_by_key = key_shards(shards)
    live_shards = only_live_shards(shards)
    for live_shard in live_shards:
        shard_chain = [live_shard]
        parent_shard_id = shard_chain[-1]["ParentShardId"]
        while parent_shard_id in shards_by_key:
            shard_chain.append(shards_by_key[parent_shard_id])
            parent_shard_id = shard_chain[-1].get("ParentShardId", "")
        yield list(reversed(shard_chain))


def refresh_live_shards(client, stream_arn: str) -> ty.Dict[str, Shard]:
    return key_shards(list(only_live_shards(yield_shards(client, stream_arn))))
