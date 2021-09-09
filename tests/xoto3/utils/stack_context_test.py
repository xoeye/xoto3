from contextvars import ContextVar
from datetime import datetime
from multiprocessing.pool import ThreadPool

from xoto3.utils.oncall_default import OnCallDefault
from xoto3.utils.stack_context import StackContext, stack_context, unwrap

NowContext = ContextVar("UtcNow", default=datetime.utcnow)


def test_stack_context():
    def final():
        return NowContext.get()()

    def intermediate():
        return final()

    sept_9 = datetime(2018, 9, 9, 9, 9, 9)

    def outer():
        with stack_context(NowContext, lambda: sept_9):
            return intermediate()

    dec_12 = datetime(2019, 12, 12, 8, 0, 0)
    with stack_context(NowContext, lambda: dec_12):
        assert dec_12 == intermediate()
        assert sept_9 == outer()
        assert dec_12 == intermediate()

    assert NowContext.get()() != sept_9
    assert NowContext.get()() != dec_12


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


NumberContext = ContextVar("Number", default=-1)


def test_threaded_stack_context():
    """The idea behind context vars is that each thread gets its own
    without anyone having to use thread.locals directly, and when paired with stack_context,
    you can have nested contexts easily within the same thread.
    """
    vals = list(range(10))

    def extract_from_context():
        return NumberContext.get()

    def put_in_context(app_func, val):
        with stack_context(NumberContext, val):
            return app_func()

    from functools import partial

    def the_app_computation():
        return extract_from_context()

    out = ThreadPool(3).map(partial(put_in_context, the_app_computation), vals)

    assert out == vals
