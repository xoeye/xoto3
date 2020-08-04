from xoto3.dynamodb.update.builders import build_update


def test_build_update():
    res = build_update(
        dict(id="i123"),
        set_attrs=dict(wasset=True, otherset="sven"),
        remove_attrs={"rem"},
        add_attrs={"num": 3},
    )

    assert res == dict(
        ExpressionAttributeNames={
            "#wasset": "wasset",
            "#rem": "rem",
            "#num": "num",
            "#otherset": "otherset",
            "#_anc_name": "id",
        },
        ExpressionAttributeValues={":wasset": True, ":addnum": 3, ":otherset": "sven"},
        Key=dict(id="i123"),
        ReturnValues="ALL_NEW",
        ConditionExpression="attribute_exists(#_anc_name)",
        UpdateExpression="SET #wasset = :wasset, #otherset = :otherset REMOVE #rem ADD #num :addnum",
    )


def test_attributes_get_clean_names():
    res = build_update(
        dict(id__="234"),
        set_attrs={"~new_attr": True, "~newattr": False},
        remove_attrs={"~old_attr"},
    )

    assert res == dict(
        ExpressionAttributeNames={
            "#newattr": "~new_attr",
            "#newattr0": "~newattr",
            "#oldattr": "~old_attr",
            "#_anc_name": "id__",
        },
        ExpressionAttributeValues={":newattr": True, ":newattr0": False},
        Key=dict(id__="234"),
        ReturnValues="ALL_NEW",
        ConditionExpression="attribute_exists(#_anc_name)",
        UpdateExpression="SET #newattr = :newattr, #newattr0 = :newattr0 REMOVE #oldattr",
    )
