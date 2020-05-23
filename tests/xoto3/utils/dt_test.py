from datetime import datetime, timezone, timedelta

from xoto3.utils.dt import iso8601strict


def test_timezones():
    dt1 = datetime(2019, 5, 6, 18, 46, 13)
    dt1s = iso8601strict(dt1)
    assert dt1s == "2019-05-06T18:46:13.000000Z"

    dt2 = datetime(2019, 5, 6, 18, 46, 13, tzinfo=timezone.utc)
    dt2s = iso8601strict(dt2)
    assert dt2s == "2019-05-06T18:46:13.000000Z"

    dt3 = datetime(2019, 5, 6, 18 - 5, 46, 13, tzinfo=timezone(timedelta(hours=-5)))
    dt3s = iso8601strict(dt3)
    assert dt3s == "2019-05-06T18:46:13.000000Z"
