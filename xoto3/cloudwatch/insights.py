import typing as ty
from datetime import datetime
from functools import partial
import string
import uuid
import os

from xoto3.utils.dt import iso8601strict
from xoto3.lam.types import LambdaContext

from .utils import (
    _DEFAULT_REGION,
    regioned_url,
    lambda_time_with_margin,
    safe_log,
    default_start_end_kwargs,
)


_ADDED_ALLOWABLE_CHARS = os.environ.get("CLOUDWATCH_INSIGHTS_ADD_ALLOWABLE_CHARS", "")

# this set will likely need to be maintained over time in the source code,
# and may be changed at runtime via environment variable
CW_INSIGHTS_ALLOWED_CHARS_SET = (
    set(string.ascii_letters) | set(string.digits) | {"_", ".", "-"} | set(_ADDED_ALLOWABLE_CHARS)
)

regioned_insights_url = partial(
    regioned_url,
    "https://console.aws.amazon.com/cloudwatch/home?region={region}#logs-insights:queryDetail=",
)


def cw_enc(any_str: str) -> str:
    """The core value encoding that CloudWatch Insights expects, which
    basically just takes chars outside a simple set and turns them into
    ASCII hex values prefixed with asterisk.
    """
    result_str = ""
    for char in any_str:
        if char in CW_INSIGHTS_ALLOWED_CHARS_SET:
            result_str += char
        else:
            result_str += "*" + "{0:02x}".format(ord(char))
    return result_str


def cw_encode_str(value: str) -> str:
    """The ' (single quote) seems to indicate "string literal" - it is unterminated"""
    return "'" + cw_enc(value)


def cw_encode_list(value: list) -> str:
    return "(~" + "~".join([cw_encode_val(item) for item in value]) + ")"


def cw_encode_val(value: ty.Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, datetime):
        return cw_encode_str(iso8601strict(value))
    if isinstance(value, list):
        return cw_encode_list(value)
    if isinstance(value, ty.Mapping):
        return cw_encode_map(value)
    return cw_encode_str(str(value))


def cw_encode_pair(key: str, value: ty.Any) -> str:
    return f"{key}~" + cw_encode_val(value)


def cw_encode_map(m: ty.Mapping[str, ty.Any]) -> str:
    return "~(" + "~".join([cw_encode_pair(k, v) for k, v in m.items()]) + ")"


def _log_group_source(groups: ty.Sequence[str]) -> dict:
    return dict(source=groups)


def _editor_query(editor_lines: ty.List[str]) -> dict:
    return dict(editorString=" \n| ".join(editor_lines))


# time utils


def last_n_seconds(n: int) -> dict:
    return dict(end=0, start=-n, timeType="RELATIVE", unit="seconds", tz="Local")


def absolute_time_range(start: datetime, end: datetime) -> dict:
    return dict(end=end, start=start, timeType="ABSOLUTE", tz="UTC")


def default_query() -> ty.List[str]:
    return ["fields @timestamp, @message", "sort @timestamp asc", "limit 200"]


def aws_request_id_query(aws_request_id: str) -> ty.List[str]:
    return [f"filter aws_request_id = '{aws_request_id}'"]


# main entry points


def insights_url_for_known_request(
    log_group_name: str,
    aws_request_id: str,
    relative_seconds: int = 3600 * 24,
    start: datetime = None,
    end: datetime = None,
    region: str = _DEFAULT_REGION,
) -> str:
    time_query = (
        absolute_time_range(start, end) if start and end else last_n_seconds(relative_seconds)
    )
    return (
        regioned_insights_url(region)
        + cw_encode_map(
            {
                **time_query,
                **_editor_query(default_query() + aws_request_id_query(aws_request_id)),
                "queryId": uuid.uuid4().hex,
                **_log_group_source([log_group_name]),
            }
        )
        + "~"  # this final char makes the URL more 'clickable' without modifying the query
    )


def insights_url_for_current_lambda_run(context: LambdaContext, **kwargs) -> str:
    """Auto-adds Lambda-relevant start/end datetimes if you don't provide
    them as keyword arguments"""
    default_start_end_kwargs(lambda_time_with_margin(), kwargs)
    return insights_url_for_known_request(context.log_group_name, context.aws_request_id, **kwargs)


safe_insights_url_for_current_lambda_run = safe_log(insights_url_for_current_lambda_run)
