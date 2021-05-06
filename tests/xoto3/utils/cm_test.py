from contextlib import contextmanager

from xoto3.utils.cm import xf_cm


@contextmanager
def yield_3():
    print("generating a 3")
    yield 3
    print("cleaning that 3 right on up")


def test_transform_context_manager():
    def add_one(x: int):
        return x + 1

    yield_4 = xf_cm(add_one)(yield_3())

    with yield_4 as actually_four:
        assert actually_four == 4
