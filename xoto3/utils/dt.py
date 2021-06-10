"""This is the most appropriate non-integer storage format for datetimes in Python.

It enforces UTC suffixed with Z rather than the verbose +00:00 format.

It's a shame this isn't in the core language.

"""
from datetime import datetime, timezone

_UTC_TZ_OFFSET = "+00:00"
_UTC_Z = "Z"


def iso8601strict(dt: datetime) -> str:
    """This works around the twin bugs in vanilla Python isoformat, which
    1) fails to postpend the 'Z' suffix, and 2) does not include the
    microseconds if the microseconds happen to be 0, both of which
    make isoformat useless for strict lexicographic sorting.
    """
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="microseconds").replace(_UTC_TZ_OFFSET, "") + _UTC_Z


def parse8601strict(dt_s: str, aware: bool = False) -> datetime:
    """Returns a datetime from the string format defined above"""
    if dt_s.endswith("Z"):
        dt_s = dt_s.replace(_UTC_Z, _UTC_TZ_OFFSET)
    val = datetime.strptime(dt_s, "%Y-%m-%dT%H:%M:%S.%f%z")
    if not aware:
        return val.replace(tzinfo=None)
    return val
