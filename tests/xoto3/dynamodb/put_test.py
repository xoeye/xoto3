import random

import pytest

import xoto3.dynamodb.put as xput


def test_put_already_exists(integration_test_id_table, integration_test_id_table_put):

    random_key = dict(id=str(random.randint(9999999, 999999999999999999999)))
    item = dict(random_key, test_item_please_ignore=True)
    integration_test_id_table_put(item)

    with pytest.raises(xput.ItemAlreadyExistsException) as ae_info:
        xput.put_but_raise_if_exists(
            integration_test_id_table,
            dict(item, new_attribute="testing attr"),
            nicename="TestThing",
        )

    assert ae_info.value.__class__.__name__ == "TestThingAlreadyExistsException"
