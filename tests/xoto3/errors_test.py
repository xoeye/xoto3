import pytest

from botocore.exceptions import ClientError

from xoto3.errors import catch_named_clienterrors


def make_raise_named_ce(Code: str):
    def raise_ce():
        raise ClientError({"Error": {"Code": Code}}, "test operation")

    return raise_ce


def test_catch_named_clienterrors_catches_only_named_errors():
    raise_mytest = make_raise_named_ce("MyTest")

    dont_except = catch_named_clienterrors(raise_mytest, ("MyTest",))

    ce, result = dont_except()
    assert ce.name == "MyTest"  # type: ignore # it *should* have a name now

    do_except = catch_named_clienterrors(raise_mytest, ("NotMyTest",))

    with pytest.raises(ClientError):
        do_except()
