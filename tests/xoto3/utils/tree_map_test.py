from decimal import Decimal

from xoto3.utils.tree_map import (
    map_tree,
    type_dispatched_transform,
    make_path_stop_transform,
    make_path_only_transform,
    compose,
)
from xoto3.utils.dec import decimal_to_number


def test_map_tree_idents():
    """Verify that certain things do and don't work"""
    lst = [1, 2, 3]
    assert map_tree(lambda x: x, lst) == lst

    t = (1, 2, 3)
    assert map_tree(lambda x: x, t) == t


def test_map_tree_type_dispatched_tx():
    tx = type_dispatched_transform({Decimal: decimal_to_number, float: int})
    d = dict(
        one=[dict(two=tuple([dict(three=Decimal("3")), "blah"]), twotwo=2, ff=4.2)], oneone="string"
    )
    dr = map_tree(tx, d)
    assert isinstance(dr["one"][0]["two"][0]["three"], int)
    assert dr == dict(
        one=[dict(two=tuple([dict(three=3), "blah"]), twotwo=2, ff=4)], oneone="string"
    )


def test_path_stop_transform():
    tp = make_path_stop_transform(("a", "four"), str)

    inp = dict(
        a=[dict(four=[1, 2, 3]), [8, 8, dict(four=(3, 3, 3))]], b=[2, 4, 6], c=dict(four=[2, 3, 4])
    )
    out = map_tree(tp, inp)
    assert out == dict(
        a=[dict(four="[1, 2, 3]"), [8, 8, dict(four="(3, 3, 3)")]],
        b=[2, 4, 6],
        c=dict(four=[2, 3, 4]),
    )


def test_path_only_transform():
    tp = make_path_only_transform(("a", "four"), str)

    inp = dict(
        a=[dict(four=[1, 2, 3]), [8, 8, dict(four=(3, 3, 3))]], b=[2, 4, 6], c=dict(four=[2, 3, 4])
    )
    out = map_tree(tp, inp)
    assert out == dict(
        a=[dict(four="[1, 2, 3]"), [8, 8, dict(four="(3, 3, 3)")]],
        b=[2, 4, 6],
        c=dict(four=[2, 3, 4]),
    )


def test_type_dispatched_path_transform():
    pathed_int_tx = make_path_only_transform(("floats_to_int",), int)
    typed_tx = type_dispatched_transform({float: pathed_int_tx, int: str})

    o = dict(floats_to_int=[1.2, 2.3, 3.4], ints_to_str=[1, 2, 3], other_floats=(1.2, 2.2))
    assert map_tree(typed_tx, o) == dict(
        floats_to_int=[1, 2, 3], ints_to_str=["1", "2", "3"], other_floats=(1.2, 2.2)
    )


def test_compose():
    itx = compose(lambda s: s + "i", str, int)
    tx = type_dispatched_transform({float: itx})

    d = dict(p=[1.2, 3.4, 8.8])

    assert map_tree(tx, d) == dict(p=["1i", "3i", "8i"])
