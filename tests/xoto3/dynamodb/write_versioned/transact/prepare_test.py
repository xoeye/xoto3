import pytest

from xoto3.dynamodb.write_versioned.prepare import parse_batch_get_request


def test_disallow_non_matching_keys():
    with pytest.raises(AssertionError):
        parse_batch_get_request(dict(tbl1=[dict(id=1), dict(other_key=2)]))


def test_deduplicate_keys():
    req = [dict(id=1), dict(id=1), dict(id=2)]

    res = parse_batch_get_request(dict(tbl1=req))
    assert res == dict(tbl1=[dict(id=1), dict(id=2)])
