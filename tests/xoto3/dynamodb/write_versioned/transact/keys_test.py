from xoto3.dynamodb.write_versioned.keys import hashable_key, hashable_key_to_key


def test_xf_keys():
    assert hashable_key(dict(id=1, group="steve")) == hashable_key(
        hashable_key_to_key(("group", "id"), ("steve", 1))
    )
