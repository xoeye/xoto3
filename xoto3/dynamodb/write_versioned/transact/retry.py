import random
import time
from logging import getLogger
from typing import Iterator

logger = getLogger(__name__)


def _choose_sleep_len_to_average_N_attempts_in_the_total_interval(
    max_attempts_before_expiration: int,
    random_sleep_length: bool,
    seconds_left: float,
    attempt: int,
) -> float:
    if attempt >= max_attempts_before_expiration:
        return -1
    return max(
        min(
            (seconds_left / (max_attempts_before_expiration - attempt))
            * (random.uniform(0.0, 1.9) if random_sleep_length else 1.0),
            seconds_left - 0.1,
        ),
        0,
    )


def timed_retry(
    transaction_expiration_seconds: float = 5.0,
    max_attempts_before_expiration: int = 25,
    random_sleep_length: bool = True,
) -> Iterator:
    attempt = 0
    expiring_at = time.monotonic() + transaction_expiration_seconds

    while attempt == 0 or time.monotonic() <= expiring_at:
        attempt += 1
        yield  # make an attempt
        msg = (
            "Attempt %d to perform transaction was beaten "
            + "by a different attempt. Sleeping for %s seconds."
        )
        sleep = _choose_sleep_len_to_average_N_attempts_in_the_total_interval(
            max_attempts_before_expiration,
            random_sleep_length,
            expiring_at - time.monotonic(),
            attempt,
        )
        if sleep < 0:
            # we've exceeded our maximum attempts and must exit
            break
        logger.warning(
            msg, attempt, f"{sleep:.3f}",
        )
        time.sleep(sleep)
