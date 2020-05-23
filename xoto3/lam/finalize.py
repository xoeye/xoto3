"""By importing this module, you request an attempt to set up an empty
array of post-runtime hooks for your Lambda."""
import typing as ty
import sys
from logging import getLogger

from xoto3.utils.env import is_aws_env


logger = getLogger(__name__)

_POST_INVOCATION_RESULT_THUNKS = list()
_POST_INVOCATION_ERROR_THUNKS = list()


def register_lambda_finalize_thunk(thunk: ty.Callable[[], None]):
    """This is how a 'client' of this system registers its desire to have a thunk called.

    No unregister ability is provided but it would be easy to implement.

    If the order in which your thunks are called matters, wrap them in a thunk and enforce the order yourself.
    """
    _POST_INVOCATION_ERROR_THUNKS.append(thunk)
    _POST_INVOCATION_RESULT_THUNKS.append(thunk)


def _run_thunks_before_function(thunks):
    # this is a closure over a list of thunks, but that list may be updated before this code runs.
    def _wrap_post_function(f):
        def inner(*args, **kwargs):
            for thunk in thunks:
                try:
                    thunk()
                except Exception as e:  # pylint: disable=broad-except
                    logger.exception(e)
            return f(*args, **kwargs)

        return inner

    return _wrap_post_function


def _noop(*_args, **_kwargs):
    pass


def __py37_finalize_hook():
    import bootstrap as aws_lambda_bootstrap  # pylint: disable=import-outside-toplevel

    aws_lambda_bootstrap.LambdaRuntimeClient.post_invocation_result = _run_thunks_before_function(
        _POST_INVOCATION_RESULT_THUNKS
    )(aws_lambda_bootstrap.LambdaRuntimeClient.post_invocation_result)

    aws_lambda_bootstrap.LambdaRuntimeClient.post_invocation_error = _run_thunks_before_function(
        _POST_INVOCATION_ERROR_THUNKS
    )(aws_lambda_bootstrap.LambdaRuntimeClient.post_invocation_error)


def __setup_lambda_finalize_hook():
    """Never call this yourself!!! It is set up by the module itself."""
    __setup_lambda_finalize_hook.__code__ = (
        _noop.__code__
    )  # setup should be idempotent/non-repeatable

    if not is_aws_env():
        logger.debug("Not registering finalize hook as this is not a Lambda environment")
        return

    try:
        if sys.version_info[0] == 3 and sys.version_info[1] == 7:
            __py37_finalize_hook()
        else:
            logger.error(
                f"No Python-version-compatible Lambda Runtime hook implementation is available for {sys.version_info}"
            )
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Could not register Lambda finalize hooks")
        logger.exception(e)


__setup_lambda_finalize_hook()  # do the one-time setup if possible
