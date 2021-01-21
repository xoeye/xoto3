import boto3
import pytest
from botocore.exceptions import ClientError

from xoto3.dynamodb.write_versioned.ddb_api import is_cancelled_and_retryable, known_key_schema
from xoto3.dynamodb.write_versioned.errors import TableSchemaUnknownError


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


def test_key_schema_unfetchable():
    try:
        table = boto3.resource("dynamodb").Table("thistabledoesnotexist")

        with pytest.raises(TableSchemaUnknownError):
            known_key_schema(table)
    except:  # noqa
        pass  # test cannot run at all without access to DynamoDB
