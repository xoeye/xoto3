# type: ignore
from datetime import datetime

from xoto3.cloudwatch.log_groups import log_group_url_for_current_lambda_run


def test_simple_format():
    lc = lambda: None  # noqa
    lc.aws_request_id = "f9031fcf-0b8e-43e3-9a87-9fd5d886aa2d"
    lc.log_group_name = "/aws/lambda/xoi-vision-nx-content-peter-verifyUserUploads"
    lc.log_stream_name = "2020/03/23/[$LATEST]317e4f226f0d4b21899bc82fa019f7cc"

    assert (
        log_group_url_for_current_lambda_run(
            lc, start=datetime(2020, 3, 23, 23, 43, 38), end=datetime(2020, 3, 23, 23, 43, 39)
        )
        == "https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logEventViewer:start=2020-03-23T23%3A43%3A38.000000Z;end=2020-03-23T23%3A43%3A39.000000Z;filter=%7B%20%24.aws_request_id%20%3D%20%22f9031fcf-0b8e-43e3-9a87-9fd5d886aa2d%22%20%7D;group=/aws/lambda/xoi-vision-nx-content-peter-verifyUserUploads;stream=2020/03/23/%5B%24LATEST%5D317e4f226f0d4b21899bc82fa019f7cc"
    )
