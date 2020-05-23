import boto3

from xoto3.utils.lazy import ThreadLocalLazy


class SessionedBoto3:
    """Apparently there are thread-safety issues conditions if you don't
    first create a session.

    https://github.com/boto/boto3/issues/1592
    """

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
    """Thread Local Lazy SessionedBoto3"""
    return ThreadLocalLazy(SessionedBoto3(method_name, *args, **kwargs))
