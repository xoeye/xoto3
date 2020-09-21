import os
import random

import pytest
import boto3

import xoto3.dynamodb.put as xput

from tests.xoto3.dynamodb.testing_utils import make_pytest_put_fixture_for_table


XOTO3_INTEGRATION_TEST_ID_TABLE_NAME = os.environ.get(
    "XOTO3_INTEGRATION_TEST_DYNAMODB_ID_TABLE_NAME"
)


_INTEGRATION_ID_TABLE = (
    boto3.resource("dynamodb").Table(XOTO3_INTEGRATION_TEST_ID_TABLE_NAME)
    if XOTO3_INTEGRATION_TEST_ID_TABLE_NAME
    else None
)
integration_table_put = make_pytest_put_fixture_for_table(_INTEGRATION_ID_TABLE)


@pytest.mark.skipif(
    not XOTO3_INTEGRATION_TEST_ID_TABLE_NAME, reason="No integration id table was defined"
)
def test_put_already_exists(integration_table_put):

    random_key = dict(id=str(random.randint(9999999, 999999999999999999999)))
    item = dict(random_key, test_item_please_ignore=True)
    integration_table_put(item)

    with pytest.raises(xput.ItemAlreadyExistsException) as ae_info:
        xput.put_but_raise_if_exists(
            _INTEGRATION_ID_TABLE, dict(item, new_attribute="testing attr"), nicename="TestThing"
        )

    assert ae_info.value.__class__.__name__ == "TestThingAlreadyExistsException"
