import time
from datetime import timedelta

from xoto3.dynamodb.write_versioned.retry import timed_retry


def test_timed_retry():
    attempted = 0

    started_at = time.monotonic()
    for _ in timed_retry(timedelta(seconds=0.5), 7):
        attempted += 1
    assert attempted == 7
    assert 0.3 < time.monotonic() - started_at < 0.5
