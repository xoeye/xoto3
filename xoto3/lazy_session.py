"""Apparently there are thread-safety issues conditions if you don't
first create a session.

https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html#multithreading-or-multiprocessing-with-sessions

https://github.com/boto/boto3/issues/1592
"""
from typing import Callable, TypeVar

import boto3.session

from xoto3.utils.lazy import ThreadLocalLazy


class SessionedBoto3:
    def __init__(self, method_name: str, *args, **kwargs):
        self.method_name = method_name
        assert self.method_name in {"resource", "client"}
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        session = boto3.session.Session()
        method = getattr(session, self.method_name)
        return method(*self.args, **self.kwargs)


def tlls(method_name: str, *args, **kwargs) -> ThreadLocalLazy[SessionedBoto3]:
    """Thread Local Lazy SessionedBoto3.

    This is deprecated in favor of tll_from_session because this one
    doesn't allow any sort of static typing to pass through.
    """
    return ThreadLocalLazy(SessionedBoto3(method_name, *args, **kwargs))


RC = TypeVar("RC")


_THREAD_SESSION = ThreadLocalLazy(lambda: boto3.session.Session())
# instead of using `resource` and `client` directly, it's recommended
# that you use this per-thread session to then create a ThreadLocalLazy resource or client.


def tll_from_session(
    resource_or_client_creator: Callable[[boto3.session.Session], RC]
) -> ThreadLocalLazy[RC]:
    """Gives your callback a threadsafe boto3 session - returns a
    ThreadLocalLazy wrapped around that callback.

    This is the currently-preferred method of generating a threadsafe
    client or resource at a module/singleton level.
    """
    return ThreadLocalLazy(lambda: resource_or_client_creator(_THREAD_SESSION()))
