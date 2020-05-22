import typing as ty
import json

import boto3


def yield_record_bodies_from_sqs_event(event: dict) -> ty.Iterable:
    for record in event["Records"]:
        yield json.loads(record["body"])


def yield_and_ack_sqs_messages(queue_url: str, limit: int = 0, accept=None) -> ty.Iterable[dict]:
    """Generates messages from an SQS queue.

    Note: this continues to yield messages until the queue is empty,
    unless limit is non-zero, in which case it will generate messages
    until *at least* that many messages are received.

    :param queue_url: URL of the SQS queue to drain.

    """
    sqs_client = boto3.client("sqs")
    recvd = 0

    while recvd < limit or limit == 0:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url, AttributeNames=["All"], MaxNumberOfMessages=10
        )

        try:
            messages = resp["Messages"]
        except KeyError:
            continue

        if accept:
            messages = [msg for msg in messages if accept(msg)]

        if not messages:
            continue

        recvd += len(messages)
        yield from messages

        entries = [
            {"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]} for msg in messages
        ]

        resp = sqs_client.delete_message_batch(QueueUrl=queue_url, Entries=entries)

        if len(resp["Successful"]) != len(entries):
            raise RuntimeError(f"Failed to delete messages: entries={entries!r} resp={resp!r}")
