from xoto3.dynamodb.stringset import stringset_contains


def test_stringset_contains():
    q_w_filter = dict(FilterExpression="does some stuff")

    tag_set = {"a", "b"}
    new_query = stringset_contains("tags", tag_set)(q_w_filter)

    assert new_query == dict(
        FilterExpression="does some stuff ( contains(#tagsSSCONTAINS, :tagsSSCONTAINS0) AND contains(#tagsSSCONTAINS, :tagsSSCONTAINS1)  ) ",
        ExpressionAttributeNames={"#tagsSSCONTAINS": "tags"},
        ExpressionAttributeValues={
            ":tagsSSCONTAINS0": list(tag_set)[0],
            ":tagsSSCONTAINS1": list(tag_set)[1],
        },
    )
