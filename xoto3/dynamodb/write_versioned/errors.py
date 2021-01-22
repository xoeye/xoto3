from xoto3.dynamodb.exceptions import DynamoDbException, DynamoDbItemException

from .types import VersionedTransaction


class TableSchemaUnknownError(KeyError):
    """If you get one of these, it means that you've attempted a write and
    we don't know how to derive the key from your item because you
    didn't give us any examples of the key as part of the option to
    prefetch items.

    There's really only one way to be certain to avoid this - specify
    an item to be prefetched by its key, so that we can assume the key
    schema based on it.

    Before this is raises, we may attempt to query DynamoDB itself for
    the key schema for your table in order to save you this extra
    work, at the cost of incurring an unexpected I/O operation partway
    through. However, depending on your IAM permissions, it may not be
    possible for us to accomplish this, in which case you will receive
    this runtime error.
    """


class ItemNotYetFetchedError(DynamoDbItemException):
    """Used internally to send control flow and information back to the
    transaction runner to prompt a lazy load of an item not already
    fetched.
    """


class TransactionAttemptsOverrun(DynamoDbException):
    """The end failure state in a system where there is high contention for resources."""

    def __init__(self, msg, failed_transaction: VersionedTransaction):
        super().__init__(msg)
        self.failed_transaction = failed_transaction
