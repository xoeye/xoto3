from xoto3.dynamodb.conditions import _range_str, item_exists, item_not_exists


def test_item_exists():
    args = dict(
        Item=dict(ya="hey"),
        ConditionExpression="whatever",
        ExpressionAttributeNames={"#nameA": "Peter"},
    )
    ie = item_exists([dict(AttributeName="group", KeyType="HASH")])

    assert ie(args) == dict(
        Item=dict(ya="hey"),
        ConditionExpression="whatever AND attribute_exists(#_anc_name)",
        ExpressionAttributeNames={"#nameA": "Peter", "#_anc_name": "group"},
    )

    assert ie(dict()) == dict(
        ConditionExpression="attribute_exists(#_anc_name)",
        ExpressionAttributeNames={"#_anc_name": "group"},
    )


def test_item_not_exists():
    args = dict(
        Item=dict(ya="hey"),
        ConditionExpression="whatever",
        ExpressionAttributeNames={"#nameA": "Peter"},
    )
    ine = item_not_exists([dict(AttributeName="group", KeyType="HASH")])

    assert ine(args) == dict(
        Item=dict(ya="hey"),
        ConditionExpression="whatever AND attribute_not_exists(#_anc_name)",
        ExpressionAttributeNames={"#nameA": "Peter", "#_anc_name": "group"},
    )


def test__range_str():
    it = iter(_range_str("hey"))
    for i in range(6):
        j = next(it)
        assert j.startswith("hey")
        assert len(j) == 3 + i
