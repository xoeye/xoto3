from botocore.exceptions import ClientError

from xoto3.dynamodb.write_versioned.ddb_api import is_cancelled_and_retryable


def test_is_cancelled_and_retryable():
    assert is_cancelled_and_retryable(
        ClientError({"Error": {"Code": "TransactionCanceledException"}}, "transact_write_items")
    )
    assert not is_cancelled_and_retryable(
        ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "transact_write_items")
    )

    assert not is_cancelled_and_retryable(
        ClientError(
            {
                "Error": {
                    "Code": "TransactionCanceledException",
                    "CancellationReasons": [
                        dict(Code="ItemCollectionSizeLimitExceeded"),  # this is not legal for retry
                        dict(Code="ConditionalCheckFailed"),
                        dict(Code="TransactionConflict"),
                        dict(Code="ThrottlingError"),
                        dict(Code="ProvisionedThroughputExceeded"),
                    ],
                }
            },
            "transact_write_items",
        )
    )
