from contextvars import ContextVar
from datetime import datetime

from xoto3.utils.oncall_default import OnCallDefault
from xoto3.utils.stack_context import StackContext, stack_context, unwrap

NowContext = ContextVar("UtcNow", default=datetime.utcnow)


def test_stack_context():
    def final():
        return NowContext.get()()

    def intermediate():
        return final()

    outer_when = datetime(2018, 9, 9, 9, 9, 9)

    def outer():
        with stack_context(NowContext, lambda: outer_when):
            return intermediate()

    way_outer_when = datetime(2019, 12, 12, 8, 0, 0)
    with stack_context(NowContext, lambda: way_outer_when):
        assert way_outer_when == intermediate()
        assert outer_when == outer()

    assert NowContext.get() != outer_when
    assert NowContext.get() != way_outer_when


def test_composes_with_oncall_default():

    when = OnCallDefault(unwrap(NowContext.get))

    @when.apply_to("now")
    def f(now: datetime = when()):
        assert isinstance(now, datetime)
        return now

    val = datetime(1922, 8, 3, 1, 2, 1)
    assert f(now=val) == val

    with stack_context(NowContext, lambda: val):
        assert val == f()
        new_val = datetime(888, 8, 8, 8, 8, 8)
        with stack_context(NowContext, lambda: new_val):
            assert new_val == f()
        assert val == f()
        assert new_val == f(new_val)

    assert f(val) == val
    assert f(val) <= datetime.utcnow()


ConsistentReadContext = StackContext("ConsistentRead", False)


def test_StackContext_interface():
    def f():
        return ConsistentReadContext()

    def g():
        return f()

    assert g() is False
    with ConsistentReadContext.set(True):
        assert g() is True
    assert g() is False
