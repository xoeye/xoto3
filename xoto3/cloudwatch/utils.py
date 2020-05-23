from typing import TypeVar, Callable, cast
import os
from datetime import datetime, timedelta
from functools import wraps
from logging import getLogger


logger = getLogger(__name__)


_DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")
MAX_LAMBDA_RUNTIME_S = 15 * 60  # 15 minutes


def regioned_url(URL_FMT: str, region: str = _DEFAULT_REGION) -> str:
    if not region:
        region = _DEFAULT_REGION
    return URL_FMT.format(region=region)


def lambda_time_with_margin() -> dict:
    margin_secs = 1
    now = datetime.utcnow()
    end = now + timedelta(seconds=margin_secs)
    start = now - timedelta(seconds=MAX_LAMBDA_RUNTIME_S + margin_secs)
    return dict(start=start, end=end)


def default_start_end_kwargs(defaults: dict, kwargs: dict):
    start = kwargs.get("start", None)
    if not start:
        kwargs["start"] = defaults["start"]
    end = kwargs.get("end", None)
    if not end:
        kwargs["end"] = defaults["end"]


SF = TypeVar("SF", bound=Callable[..., str])


def safe_log(func: SF) -> SF:
    @wraps(func)
    def wrapper(*args, **kwargs) -> str:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            return ""

    return cast(SF, wrapper)
