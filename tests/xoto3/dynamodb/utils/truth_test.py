from decimal import Decimal
from xoto3.dynamodb.utils.truth import dynamodb_truthy


def test_dynamodb_truthy():
    # empty collections are not truthy, nor is the empty string
    assert not dynamodb_truthy(set())
    assert not dynamodb_truthy(list())
    assert not dynamodb_truthy(dict())
    assert not dynamodb_truthy("")
    assert not dynamodb_truthy(False)

    # numbers are truthy for Dynamo regardless of value
    assert dynamodb_truthy(0)
    assert dynamodb_truthy(0.0)
    assert dynamodb_truthy(Decimal(0.0))
    assert dynamodb_truthy({0})
    assert dynamodb_truthy([0])
    assert dynamodb_truthy(" ")
    assert dynamodb_truthy("0")
    assert dynamodb_truthy({""})
