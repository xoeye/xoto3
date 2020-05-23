from datetime import datetime
from functools import partial
import urllib.parse

from xoto3.utils.dt import iso8601strict
from xoto3.lam.types import LambdaContext

from .utils import (
    _DEFAULT_REGION,
    regioned_url,
    safe_log,
    lambda_time_with_margin,
    default_start_end_kwargs,
)


_regioned_log_group_url = partial(
    regioned_url, "https://console.aws.amazon.com/cloudwatch/home?region={region}#logEventViewer:"
)


def _fmt_key_val(key: str, val: str) -> str:
    return f"{key}={urllib.parse.quote(val)}"


def _absolute_time_range(start: datetime, end: datetime) -> dict:
    return dict(start=iso8601strict(start), end=iso8601strict(end))


def _query_aws_request_id(aws_request_id: str) -> dict:
    return dict(filter="{ " + f'$.aws_request_id = "{aws_request_id}"' + " }")


def log_group_url_for_known_request(
    log_group_name: str,
    log_stream_name: str,
    aws_request_id: str,
    start: datetime,
    end: datetime,
    region: str = _DEFAULT_REGION,
) -> str:
    return _regioned_log_group_url(region) + ";".join(
        [
            _fmt_key_val(key, val)
            for key, val in {
                **_absolute_time_range(start, end),
                **_query_aws_request_id(aws_request_id),
                **dict(group=log_group_name, stream=log_stream_name),
            }.items()
        ]
    )


@safe_log
def log_group_url_for_current_lambda_run(context: LambdaContext, **kwargs) -> str:
    """Auto-adds Lambda-relevant start/end datetimes if you don't provide
    them as keyword arguments"""
    default_start_end_kwargs(lambda_time_with_margin(), kwargs)
    return log_group_url_for_known_request(
        context.log_group_name, context.log_stream_name, context.aws_request_id, **kwargs
    )
