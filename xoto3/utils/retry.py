import time
import typing as ty
from functools import wraps

F = ty.TypeVar("F", bound=ty.Callable)


def expo(length: int = -1, y: float = 1.0,) -> ty.Iterator[float]:
    """Ends iteration after 'length'.

    If you want infinite exponential values, pass a negative number for 'length'.
    """
    count = 0
    while length < 0 or count < length:
        yield 2 ** count * y
        count += 1


def sleep_join(
    seconds_iter: ty.Iterable[float], sleep: ty.Callable[[float], ty.Any] = time.sleep
) -> ty.Iterator:
    """A common base strategy for separating retries by sleeps."""
    yield
    for secs in seconds_iter:
        sleep(secs)
        yield


IsRetryable = ty.Callable[[Exception], bool]
RetryStrategy = ty.Iterable[IsRetryable]
RetryStrategyFactory = ty.Callable[[], RetryStrategy]


def retry_while(strategy_factory: RetryStrategyFactory) -> ty.Callable[[F], F]:
    """Uses your retry strategy every time an exception is raised.

    Your iterable can therefore provide different handling for each
    incrementing error, as well as configurable delays between errors,
    etc.
    """

    def _retry_decorator(func: F) -> F:
        @wraps(func)
        def retry_wrapper(*args, **kwargs):
            for is_retryable in strategy_factory():
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not is_retryable(e):
                        raise

        return ty.cast(F, retry_wrapper)

    return _retry_decorator


def sleep_between_expected_failures(
    is_retryable: IsRetryable, seconds: ty.Iterable[float],
) -> RetryStrategyFactory:
    return lambda: (is_retryable for _ in sleep_join(seconds))
