from xoto3.dynamodb.exceptions import DynamoDbException, DynamoDbItemException


class TableUnknownToTransactionError(KeyError):
    pass


class ItemUnknownToTransactionError(DynamoDbItemException):
    pass


class TransactionAttemptsOverrun(DynamoDbException):
    pass
