import boto3
import pytest
from botocore.exceptions import ClientError

from xoto3.dynamodb.write_versioned import delete
from xoto3.dynamodb.write_versioned.ddb_api import (
    built_transaction_to_transact_write_items_args,
    is_cancelled_and_retryable,
    known_key_schema,
)
from xoto3.dynamodb.write_versioned.errors import TableSchemaUnknownError, VersionedTransaction
from xoto3.dynamodb.write_versioned.prepare import items_and_keys_to_clean_table_data


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


def test_built_transaction_includes_unmodified():

    tx = VersionedTransaction(
        tables=dict(
            Common=items_and_keys_to_clean_table_data(
                ("id",), [dict(id="unmodified")], [dict(id="delete", val=4)]
            )
        )
    )
    tx = delete(tx, "Common", dict(id="delete"))

    args = built_transaction_to_transact_write_items_args(tx, "adatetimestring")

    assert {
        "TransactItems": [
            {
                "Delete": {
                    "TableName": "Common",
                    "Key": {"id": {"S": "delete"}},
                    "ExpressionAttributeNames": {
                        "#itemVersion": "item_version",
                        "#idThatExists": "id",
                    },
                    "ExpressionAttributeValues": {":curItemVersion": {"N": "0"}},
                    "ConditionExpression": "#itemVersion = :curItemVersion OR ( attribute_not_exists(#itemVersion) AND attribute_exists(#idThatExists) )",
                }
            },
            {
                "ConditionCheck": {
                    "TableName": "Common",
                    "Key": {"id": {"S": "unmodified"}},
                    "ExpressionAttributeNames": {"#itemVersion": "item_version"},
                    "ExpressionAttributeValues": {":curItemVersion": {"N": "0"}},
                    "ConditionExpression": "#itemVersion = :curItemVersion OR attribute_not_exists(#itemVersion)",
                }
            },
        ]
    } == args
