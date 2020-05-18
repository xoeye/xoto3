"""This is the most appropriate non-integer storage format for datetimes in Python.

It enforces UTC suffixed with Z rather than the verbose +00:00 format.

It's a shame this isn't in the core language.

"""
from datetime import datetime, timezone

_UTC_TZ_HRS = "+00:00"


def iso8601strict(dt: datetime) -> str:
    """This works around the twin bugs in vanilla Python isoformat which
    fail to postpend the 'Z' suffix, and do not print the microseconds
    if the microseconds happen to be 0.
    """
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="microseconds").replace(_UTC_TZ_HRS, "") + "Z"
