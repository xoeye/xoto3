import os
import random
from collections import defaultdict

import pytest

import boto3

from xoto3.dynamodb.update import versioned_diffed_update_item
from xoto3.dynamodb.utils.expressions import add_variables_to_expression
from xoto3.dynamodb.utils.table import table_primary_keys, extract_key_from_item


_TEST_TABLE_NAME = os.environ.get("XOTO3_TEST_DYNAMODB_TABLE_NAME", "")


def test_add_variables_to_expression():
    variables = dict(deletedAt="2020-01-01T00:00:00.000000Z", do_a_thing="okay")

    query_dict = add_variables_to_expression(dict(), variables)

    assert query_dict["ExpressionAttributeNames"] == {
        "#deletedAt": "deletedAt",
        "#do_a_thing": "do_a_thing",
    }
    assert query_dict["ExpressionAttributeValues"] == {
        ":deletedAt": "2020-01-01T00:00:00.000000Z",
        ":do_a_thing": "okay",
    }


def test_add_variables_to_expression_with_bad_attribute_name():
    variables = {"thingy": "THINGY", "~deleted__At": "2020-02-02T00:00:00.000000Z"}

    with pytest.raises(
        ValueError, match="Attribute name contains invalid characters: '~deleted__At'"
    ):
        add_variables_to_expression(dict(), variables)


def test_add_variables_to_expression_with_duplicate_attribute_name():
    query_dict = dict(ExpressionAttributeNames={"#deletedAt": "deletedAt"})
    variables = dict(deletedAt="2020-02-02T00:00:00.000000Z")

    with pytest.raises(
        ValueError, match="Cannot add a duplicate expression attribute name #deletedAt"
    ):
        add_variables_to_expression(query_dict, variables)


def test_add_variables_to_expression_with_duplicate_attribute_value():
    query_dict = dict(ExpressionAttributeValues={":deletedAt": "2020-01-01T00:00:00.000000Z"})
    variables = dict(deletedAt="2020-02-02T00:00:00.000000Z")

    with pytest.raises(
        ValueError, match="Cannot add a duplicate expression attribute value :deletedAt"
    ):
        add_variables_to_expression(query_dict, variables)


@pytest.fixture
def fix_item():
    items_to_clean_by_table_name = defaultdict(list)

    def _create_item(table, item: dict):
        table.put_item(Item=item)
        items_to_clean_by_table_name[table.name].append(item)
        return item

    yield _create_item

    ddb = boto3.resource("dynamodb")
    for table_name, items in items_to_clean_by_table_name.items():
        for item in items:
            table = ddb.Table(table_name)
            table.delete_item(Key=extract_key_from_item(table, item))


@pytest.mark.integ
def test_expression_attributes_against_dynamodb(fix_item):
    assert _TEST_TABLE_NAME, "Cannot test without an available table"
    table = boto3.resource("dynamodb").Table(_TEST_TABLE_NAME)

    item_random_key = {
        attr: "xoto3-integ-test" + str(random.randint(0, 99999999999))
        for attr in table_primary_keys(table)
    }
    # requires string attributes for the primary key because i'm too
    # lazy to make this key generation fancier for a test.

    bad_attr = "~known-bad*chars"
    fix_item(table, {**item_random_key, **{bad_attr: "some random data"}})

    def del_bad_attr(item):
        item.pop(bad_attr, None)
        return item

    result = versioned_diffed_update_item(table, del_bad_attr, item_random_key)
    assert bad_attr not in result
