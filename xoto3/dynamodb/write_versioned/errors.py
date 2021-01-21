from xoto3.dynamodb.exceptions import DynamoDbException, DynamoDbItemException


class TableSchemaUnknownError(KeyError):
    """If you get one of these, it means that you've attempted a write and
    we don't know how to derive the key from your item because you didn't
    give us any examples of the key as part of the option to prefetch
    items.

    There's really only one way to fix this - specify an item by key
    upfront, so that we can derive the key schema from it.

    In the future, we may attempt to query DynamoDB itself for the key
    schema for your table in order to save you this extra work, at the
    cost of incurring an unexpected I/O operation partway through.
    """


class ItemNotYetFetchedError(DynamoDbItemException):
    pass


class TransactionAttemptsOverrun(DynamoDbException):
    pass
