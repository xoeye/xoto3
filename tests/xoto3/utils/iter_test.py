import pytest

from xoto3.utils.iter import peek


def test_peek():
    lst = [1, 2, 3]
    is_empty, first, li_1 = peek(lst)
    assert not is_empty
    assert first == 1
    assert next(li_1) == 1

    is_empty, first, li_2 = peek(li_1)
    assert not is_empty
    assert first == 2
    assert next(li_2) == 2

    is_empty, first, li_3 = peek(li_2)
    assert not is_empty
    assert first == 3
    assert next(li_3) == 3

    is_empty, first, li_4 = peek(li_3)
    assert is_empty
    assert first is None
    with pytest.raises(StopIteration):
        next(li_4)
