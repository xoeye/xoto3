import pytest
from botocore.exceptions import ClientError

from xoto3.backoff import backoff


def make_named_ce(Code: str):
    return ClientError({"Error": {"Code": Code}}, "test operation")


def test_backoff_some_client_errors():
    count = 0

    @backoff
    def fails_twice():
        nonlocal count
        if count > 1:
            return "done"
        count += 1
        raise make_named_ce("ThrottlingException")

    assert "done" == fails_twice()
    assert count == 2


def test_dont_backoff_others():
    @backoff
    def not_found():
        raise make_named_ce("NotFound")

    with pytest.raises(ClientError):
        not_found()

    @backoff
    def whoops():
        raise Exception("whoops")

    with pytest.raises(Exception):
        whoops()
