from xoto3.cloudwatch.logs.events import CLOUDWATCH_LOGS_FILTER_LOG_EVENTS
from xoto3.paginate import yield_pages_from_operation


class FakeApi:
    def __init__(self, results):
        self.calls = 0
        self.results = results

    def __call__(self, **kwargs):
        self.calls += 1
        return self.results[self.calls - 1]


def test_pagination_with_nextToken_and_limit():

    fake_cw = FakeApi(
        [
            dict(nextToken="1", events=[1, 2, 3]),
            dict(nextToken="2", events=[4, 5, 6]),
            dict(nextToken="3", events=[7, 8, 9]),
        ]
    )

    nt = None

    def le_cb(next_token):
        nonlocal nt
        nt = next_token

    collected_events = list()
    for page in yield_pages_from_operation(
        *CLOUDWATCH_LOGS_FILTER_LOG_EVENTS, fake_cw, dict(limit=6), last_evaluated_callback=le_cb
    ):
        for event in page["events"]:
            collected_events.append(event)

    assert collected_events == list(range(1, 7))

    assert nt == "2"
