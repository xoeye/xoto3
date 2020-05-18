import time
from logging import getLogger as get_logger

import botocore.exceptions

from xoto3.errors import client_error_name


logger = get_logger(__name__)


RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")


def backoff(func):
    """Will retry a boto3 operation closure until it succeeds, as long
    as the exception was throughput-related.
    """

    def backoff_wrapper(*args, **kwargs):
        retries = 0
        pause_time = 0

        while True:
            try:
                return func(*args, **kwargs)
            except botocore.exceptions.ClientError as ce:
                if client_error_name(ce) not in RETRY_EXCEPTIONS:
                    raise
                pause_time = 2 ** retries
                logger.info("Back-off set to %d seconds", pause_time)
                time.sleep(pause_time)
                retries += 1

    return backoff_wrapper
