from botocore.exceptions import ClientError

from xoto3.errors import client_error_name

_KNOWN_RETRYABLE_UPDATE_ERRORS = (
    "TransactionConflictException",
    "ConditionalCheckFailedException",
)


def is_conditional_update_retryable(ce: ClientError) -> bool:
    return client_error_name(ce) in _KNOWN_RETRYABLE_UPDATE_ERRORS
