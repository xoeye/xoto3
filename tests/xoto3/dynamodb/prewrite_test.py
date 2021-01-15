from decimal import Decimal

import pytest
from boto3.dynamodb.types import Binary

from xoto3.dynamodb.prewrite import dynamodb_prewrite
from xoto3.dynamodb.utils.serde import deserialize_item, serialize_item


def test_dynamodb_prewrite_still_have_empty_strings_in_lists():
    d = dict(f=["peter", "gaultney", "", None])
    assert dynamodb_prewrite(d) == dict(f=["peter", "gaultney", "", None])


def test_prewrite_no_top_level_empty_string_values():
    d = dict(a="", b="23", c=dict(f=""))
    assert dynamodb_prewrite(d) == dict(b="23", c=dict(f=""))


def test_dynamodb_prewrite_empty_strings_allowed_in_sets():
    d = dict(g={"peter", "gaultney", ""})
    assert dynamodb_prewrite(d) == dict(g={"peter", "gaultney", ""})


def test_tuples_to_lists():
    d = dict(b=[(1, 2, 3), (3, 4, 5)])
    assert dynamodb_prewrite(d) == dict(b=[[1, 2, 3], [3, 4, 5]])


def test_strip_falsy_top_level():
    d = dict(a="yes", b=False, c="", d=set(), e=list(), f=dict(), g=dict(h=False))
    assert dynamodb_prewrite(d) == dict(a="yes", g=dict(h=False))


def test_dynamodb_prewrite():
    """Dynamo won't let you write certain things that have 'reliable' defaults,
    such as empty sets and empty strings.  So we have a utility to strip keys
    with those values recursively, but ONLY those values.
    """
    test_dict = dict(
        key0=0.0,  # kept
        key1="string val remains",  # kept
        key2=0,  # kept
        key3=1,  # kept
        key4=True,  # kept
        key5=False,  # stripped
        key6="",  # stripped,
        key7=set(),  # stripped,
        key8=dict(),  # stripped
        key9=list(),  # stripped
    )
    with pytest.raises(TypeError):
        serialize_item(test_dict)
    SPLIT = 5

    stripped = dynamodb_prewrite(test_dict)
    for i in range(0, SPLIT):
        assert f"key{i}" in stripped
    for i in range(SPLIT, len(test_dict.keys())):
        assert f"key{i}" not in stripped

    serialize_item(stripped)


def test_more_prewrite():
    test_dict = dict(
        k0={},
        k1={"nonempty string"},
        k2={"nonempty string", ""},
        k3=tuple(),
        k4=tuple([1, 2, 3]),
        k5=tuple(["", "nonempty string", ""]),
        k6=dict(k1=[dict(j1={"nonempty", "blah"})]),  # nested set
        k7=dict(k1=tuple([dict(j1={"nonempty", "blah"})])),  # nested tuple gets turned into list
        k8={1, 2, 3, Decimal("3.1415926535")},  # NS
        k9={Binary(b"123"), Binary(b"456")},  # BS
    )

    # this used to error consistently, but a boto3 upgrade (somewhere around 1.16?) has made it stop erroring.
    #
    # with pytest.raises(TypeError):
    #     serialize_item(test_dict)

    out = dynamodb_prewrite(test_dict)
    out_ser = serialize_item(out)  # doesn't raise
    # assert out.keys() == test_dict.keys()

    assert out["k2"] == test_dict["k2"]
    assert "k3" not in out
    assert out["k4"] == [1, 2, 3]
    assert out["k5"] == ["", "nonempty string", ""]
    assert out["k6"] == test_dict["k6"]
    assert out["k7"] == test_dict["k6"]  # tuple was transformed into list but otherwise same
    assert out["k8"] == test_dict["k8"]
    assert out["k9"] == test_dict["k9"]

    out_deser = deserialize_item(out_ser)
    assert out_deser == out
