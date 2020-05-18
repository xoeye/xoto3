"""Utilities for living with boto3 errors"""

from typing import Sequence, Optional, Any, Callable, Tuple
from functools import wraps

from botocore.exceptions import ClientError


def client_error_name(client_error: ClientError):
    """Take a botocore ClientError and return its string name"""
    return client_error.response.get("Error", {}).get("Code")


def catch_named_clienterrors(
    func, names: Sequence[str] = ()
) -> Callable[..., Tuple[Optional[ClientError], Any]]:
    """Function decorator that catches and returns all named ClientErrors

    but passes/re-raises any that do not match these names.

    This is basically a way of making your code less ugly by
    specifying up front the types of errors you're interested in
    actually handling, instead of having to do all that error checking
    in the 'except' block.

    Think of it as providing a way to catch more specific exceptions
    when there are no actualy exception types for what you want to
    catch.  Basically, if boto3 were better about throwing named
    exceptions, this would obviously be unnecessary.

    If names is empty, catch and return all ClientErrors.

    """
    assert not isinstance(names, str)
    name_set = set(names)

    @wraps(func)
    def catch_clienterrors(*args, **kwargs) -> Tuple[Optional[ClientError], Any]:
        try:
            return None, func(*args, **kwargs)
        except ClientError as ce:
            ce_name = client_error_name(ce)
            ce.name = ce_name  # making this easier to access
            if not name_set or ce_name in name_set:
                return ce, None
            raise ce

    return catch_clienterrors
