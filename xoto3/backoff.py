"""Infinite exponential backoff for AWS API throttling.

See retry.py for more general purpose retry strategy utilities.

"""
from logging import getLogger as get_logger

import botocore.exceptions

from .errors import client_error_name
from .utils.retry import expo, retry_while, sleep_join

logger = get_logger(__name__)


RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")


def _is_boto3_retryable(e: Exception) -> bool:
    if not isinstance(e, botocore.exceptions.ClientError):
        return False
    return client_error_name(e) in RETRY_EXCEPTIONS


backoff = retry_while(_is_boto3_retryable for _ in sleep_join(expo()))
"""Infinite exponential backoff for the specified errors"""
