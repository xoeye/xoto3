import pytest

from xoto3.dynamodb.utils.expressions import add_variables_to_expression


def test_add_variables_to_expression():
    query_dict = dict()
    variables = dict(deletedAt="2020-01-01T00:00:00.000000Z")

    add_variables_to_expression(query_dict, variables)

    assert query_dict["ExpressionAttributeNames"] == {"#deletedAt": "deletedAt"}
    assert query_dict["ExpressionAttributeValues"] == {":deletedAt": "2020-01-01T00:00:00.000000Z"}


def test_add_variables_to_expression_with_bad_attribute_name():
    query_dict = dict()
    variables = dict(thingy="THINGY", deleted__At="2020-02-02T00:00:00.000000Z")

    with pytest.raises(
        ValueError, match="Attribute name contains invalid characters: 'deleted__At'"
    ):
        add_variables_to_expression(query_dict, variables)


def test_add_variables_to_expression_with_duplicate_attribute_name():
    query_dict = dict(ExpressionAttributeNames={"#deletedAt": "deletedAt"},)
    variables = dict(deletedAt="2020-02-02T00:00:00.000000Z")

    with pytest.raises(
        ValueError, match="Cannot add a duplicate expression attribute name #deletedAt"
    ):
        add_variables_to_expression(query_dict, variables)


def test_add_variables_to_expression_with_duplicate_attribute_value():
    query_dict = dict(ExpressionAttributeValues={":deletedAt": "2020-01-01T00:00:00.000000Z"},)
    variables = dict(deletedAt="2020-02-02T00:00:00.000000Z")

    with pytest.raises(
        ValueError, match="Cannot add a duplicate expression attribute value :deletedAt"
    ):
        add_variables_to_expression(query_dict, variables)
