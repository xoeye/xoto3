import time
import typing as ty
from datetime import datetime
from logging import getLogger

from typing_extensions import TypedDict

from xoto3.paginate import PagePathsTemplate, yield_pages_from_operation

_log = getLogger(__name__)


class LogEvent(TypedDict):
    timestamp: int
    message: str
    ingestionTime: int


class FilteredLogEvent(LogEvent, total=False):
    logStreamName: str
    eventId: str


CLOUDWATCH_LOGS_FILTER_LOG_EVENTS = PagePathsTemplate(
    ("nextToken",), ("nextToken",), ("limit",), ("events",),
)


def yield_filtered_log_events(
    client,
    log_group_name: str,
    start_time: datetime,
    filter_pattern: str = "",
    *,
    end_time: ty.Optional[datetime] = None,
    watch_interval: float = 1.0,
    # set to a negative number or 0 to stop yielding events when
    # the end of the stream is reached.
) -> ty.Iterator[FilteredLogEvent]:
    """This will iterate infinitely unless watch_interval is set to 0 or
    negative, or end_time is provided.
    """
    req = dict(logGroupName=log_group_name, startTime=int(start_time.timestamp() * 1000))
    if filter_pattern:
        req["filterPattern"] = filter_pattern
    if end_time:
        end_utc = end_time.timestamp()
        req["endTime"] = int(end_utc * 1000)
    else:
        end_utc = 0

    def intercept_nextToken(next_token):
        """When filter_log_events returns an empty nextToken, that means it
        knows of nothing further in the log group.  But it turns out
        you can hang on to the previous nextToken and resume calling
        later on, and you should pick up where you left off with new
        log events.

        We don't want to do that, however, if we're already past the
        end time specified by the caller and we're receiving empty
        nextTokens, meaning that we've completed our scan.
        """

        if next_token or (end_utc and end_utc < datetime.utcnow().timestamp()):
            req["nextToken"] = next_token

    while True:
        pages = yield_pages_from_operation(
            *CLOUDWATCH_LOGS_FILTER_LOG_EVENTS,
            client.filter_log_events,
            req,
            last_evaluated_callback=intercept_nextToken,
        )
        for page in pages:
            _log.debug(str(req))
            yield from page["events"]
        if watch_interval <= 0:
            break
        if req.get("nextToken") is None:
            break
        time.sleep(watch_interval)
