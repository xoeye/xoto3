from datetime import datetime, timedelta
from functools import partial

import boto3

from xoto3.paginate import yield_pages_from_operation

cw_client = boto3.client("logs")

start_time = (datetime.utcnow() - timedelta(hours=20)).timestamp() * 1000
end_time = datetime.utcnow().timestamp() * 1000

query = dict(
    logGroupName="xoi-ecs-logs-devl",
    logStreamNamePrefix="dataplateocr/dataplateocrContainer",
    startTime=int(start_time),
    endTime=int(end_time),
)

nt = ("nextToken",)
CLOUDWATCH_FILTER_LOG_EVENTS = (
    nt,
    nt,
    ("limit",),
    ("events",),
)

yield_cloudwatch_pages = partial(yield_pages_from_operation, *CLOUDWATCH_FILTER_LOG_EVENTS,)


for page in yield_cloudwatch_pages(cw_client.filter_log_events, query,):
    for event in page["events"]:
        print(event["message"])
