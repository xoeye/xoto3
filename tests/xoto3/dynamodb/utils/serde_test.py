from xoto3.dynamodb.utils.serde import dynamodb_prewrite_empty_str_in_dict_to_null_transform


def test_no_empty_strings_in_maps():
    d = dict(a="", b="b")
    assert dynamodb_prewrite_empty_str_in_dict_to_null_transform(d) == dict(a=None, b="b")
