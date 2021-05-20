# pylint: disable=unused-argument,unused-variable
from datetime import datetime

import pytest

from xoto3.utils.oncall_default import NotSafeToDefaultError, OnCallDefault

utcnow = OnCallDefault(datetime.utcnow)


def test_oncall_default_works_with_pos_or_kw():
    @utcnow.apply_to("when")
    def final(a: str, when: datetime = utcnow(), f: float = 1.2):
        return when

    assert final("a") <= utcnow()

    val = datetime(1888, 8, 8, 8, 8, 8)
    assert val == final("a", when=val)
    assert val == final("c", f=4.2, when=val)


def test_oncall_default_works_with_kw_only():
    @utcnow.apply_to("when")
    def f(a: str, *, when: datetime = utcnow()):
        return when

    val = datetime(1900, 1, 1, 11, 11, 11)
    assert val == f("3", when=val)


def test_deco_works_with_var_kwargs():
    @utcnow.apply_to("when")
    def f(**kwargs):
        return kwargs["when"]

    assert datetime.utcnow() <= f()
    assert f() <= datetime.utcnow()

    direct = datetime(2012, 12, 12, 12, 12, 12)
    assert direct == f(when=direct)


def test_disallow_positional_without_default():
    """A positional-possible argument without a default could have a
    positional argument provided after it and then we'd be unable to tell
    for sure whether it had been provided intentionally.
    """

    with pytest.raises(NotSafeToDefaultError):

        @utcnow.apply_to("when")
        def nope(when: datetime, a: int):
            pass


def test_disallow_not_found_without_var_kwargs():

    with pytest.raises(NotSafeToDefaultError):

        @utcnow.apply_to("notthere")
        def steve(a: str, *args, b=1, c=2):
            pass


def test_disallow_var_args_name_matches():
    with pytest.raises(NotSafeToDefaultError):
        # *args itself has the default value 'new empty tuple', and if
        # you want to provide a positional default you should give it
        # a real name.
        @utcnow.apply_to("args")
        def felicity(a: str, *args):
            pass

    with pytest.raises(NotSafeToDefaultError):

        # kwargs itself has the default value 'new empty dict', and if
        # you want to put things in it you should specify them
        # individually.
        @utcnow.apply_to("kwargs")
        def george(a: str, **kwargs):
            pass
