from xoto3.dynamodb.prewrite import dynamodb_prewrite


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
