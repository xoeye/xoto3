import copy
from datetime import datetime
from decimal import Decimal

from xoto3.dynamodb.update.diff import (
    is_meaningful_value_update,
    build_update_diff,
    select_attributes_for_set_and_remove,
)


def test_is_meaningful_value_update():
    assert not is_meaningful_value_update(None, dict())
    assert not is_meaningful_value_update(None, list())
    assert not is_meaningful_value_update(None, "")
    assert not is_meaningful_value_update(None, set())
    assert not is_meaningful_value_update(None, False)
    assert not is_meaningful_value_update(None, None)
    assert not is_meaningful_value_update(dict(), None)
    assert not is_meaningful_value_update(set(), None)
    assert not is_meaningful_value_update("peter", "peter")

    assert is_meaningful_value_update("nonempty", None)
    assert is_meaningful_value_update(None, "nonempty")
    assert is_meaningful_value_update(False, True)
    assert is_meaningful_value_update(True, False)


def test_build_update_diff():
    d1 = {
        "false": False,
        "key1": {"inner": {1, 2, 3, 4}, "inner2": [3, 4, 5, 6]},
        "key2": 4,
        "key3": ["two", "peter", "four"],
        "key4": "string",
    }

    assert not build_update_diff(d1, copy.deepcopy(d1))

    d2 = copy.deepcopy(d1)
    d2["key4"] = "integer"
    assert set(build_update_diff(d1, d2).keys()) == {"key4"}

    d2["key1"]["inner3"] = "test new key"  # type: ignore
    assert set(build_update_diff(d1, d2).keys()) == {"key4", "key1"}

    d3 = copy.deepcopy(d1)
    d3["key3"] = ["two", 3, 4]
    d3["key1"]["inner"].pop()  # type: ignore

    d3_d1_diff = build_update_diff(d1, d3)
    assert set(d3_d1_diff.keys()) == {"key3", "key1"}
    assert d3_d1_diff["key3"] == ["two", 3, 4]
    assert d3_d1_diff["key1"]["inner"] == {2, 3, 4}
    assert d3_d1_diff["key1"]["inner2"] == [3, 4, 5, 6]

    d3.pop("key2")
    d3_rem_k2_d1_diff = build_update_diff(d1, d3)
    assert set(d3_rem_k2_d1_diff.keys()) == {"key2", "key3", "key1"}
    assert d3_rem_k2_d1_diff["key2"] is None

    d4 = copy.deepcopy(d1)
    assert not build_update_diff(d1, d4)
    d4.pop("key2")  # removed
    d4["key5"] = 8
    d4_d1_diff = build_update_diff(d1, d4)
    assert d4_d1_diff["key2"] is None
    assert d4_d1_diff["key5"] == 8
    d1_d4_diff = build_update_diff(d4, d1)
    assert d1_d4_diff["key2"] == 4
    assert d1_d4_diff["key5"] is None

    # ignore worthless additions of empty values
    d5 = copy.deepcopy(d1)
    d5["empty_str"] = ""
    d5["null_value"] = None
    d5["empty_list"] = []
    d5["empty_set"] = set()
    d5["externalContent"] = list()
    d5["false_bool"] = False
    assert not build_update_diff(d1, d5)

    # do consider key removals to be a diff
    d6 = copy.deepcopy(d1)
    d6["empty_valkey"] = ""
    d1_d6 = build_update_diff(d6, d1)
    assert d1_d6["empty_valkey"] is None


def test_diff_coerces_unacceptable_types_by_default():
    """You would never expect the old item to be anything other than pure
    DynamoDB data, but the new item might have had things added that
    won't work in DynamoDB without transformation.
    """
    old = dict(topA=[1, 2, 3], topB=dict(M="2018-04-13T12:23:34.000122Z"))
    new = dict(topA=(1, 2, 3), topB=dict(M=datetime(2018, 4, 13, 12, 23, 34, 122)), newC=1.0)

    diff = build_update_diff(old, new)
    assert set(diff.keys()) == {"newC"}
    assert diff["newC"] == Decimal(1.0)
    assert isinstance(diff["newC"], Decimal)


def test_select_attributes():
    d1 = {
        "key1": {"inner": {1, 2, 3, 4}, "inner2": [3, 4, 5, 6]},
        "key2": 4,
        "key3": ["two", "peter", "four"],
        "key4": "string",
        "d1-empty-val": [],
        "d1-none-val": None,
    }
    d2 = copy.deepcopy(d1)
    d2.pop("key1")  # removed
    d2["key2"] = 7  # changed
    d2["key5"] = [3, 4, 5]  # added
    d2["d1-none-val"] = list()  # not a real change
    d2.pop("d1-empty-val")  # removal even though it was previously empty
    d2["d2-empty-val"] = list()  # this is not a proper add - ignore it

    d1_d2_diff = build_update_diff(d1, d2)
    attrs = select_attributes_for_set_and_remove(d1_d2_diff)
    assert set(attrs["set_attrs"].keys()) == {"key2", "key5"}
    assert attrs["remove_attrs"] == {"key1", "d1-empty-val"}

    # try the reverse!
    attrs = select_attributes_for_set_and_remove(build_update_diff(d2, d1))
    assert set(attrs["set_attrs"].keys()) == {"key1", "key2"}
    assert attrs["remove_attrs"] == {"key5", "d2-empty-val"}

    # note that in neither case was 'di-none-val' removed because
    # it appears in both objects and the key was not removed from either


def test_full_diffed_update_remove_new_dangerous_empty_strings():
    content = dict(id="1234", group="okaygroup", box=[1, 2, 3], empty_thing=list(), empty_string="")

    uc = copy.deepcopy(content)
    uc["group"] = ""
    uc["newthing"] = 1

    assert select_attributes_for_set_and_remove(build_update_diff(content, uc)) == dict(
        set_attrs=dict(newthing=1), remove_attrs={"group"}
    )
