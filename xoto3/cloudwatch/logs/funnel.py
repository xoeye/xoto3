import threading
import typing as ty
from datetime import datetime, timezone

from xoto3.utils.multicast import Cleanup

from .events import LogEvent, yield_filtered_log_events

LogEventFunnel = ty.Callable[[LogEvent], None]


def funnel_latest_from_log_group(
    cloudwatch_logs_client, log_group_name: str, log_event_funnel: LogEventFunnel,
) -> Cleanup:
    start = datetime.now(timezone.utc)
    bottle = dict(poisoned=False)

    def put_logs_into_funnel():
        for log_event in yield_filtered_log_events(cloudwatch_logs_client, log_group_name, start):
            if bottle["poisoned"]:
                break
            log_event_funnel(log_event)

    thread = threading.Thread(target=put_logs_into_funnel, daemon=True)
    thread.start()

    def poison():
        bottle["poisoned"] = True

    return poison
