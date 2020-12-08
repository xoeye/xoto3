"""Wrappers for CloudWatch metrics"""

# References for much of this can be found at
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.put_metric_data

import functools
import typing as ty
from datetime import datetime
from logging import getLogger

import botocore
from typing_extensions import TypedDict

from xoto3.lazy_session import tlls

logger = getLogger(__name__)


CLOUDWATCH_CLIENT = tlls("client", "cloudwatch")


MetricUnits = frozenset(
    [
        "Seconds",
        "Microseconds",
        "Milliseconds",
        "Bytes",
        "Kilobytes",
        "Megabytes",
        "Gigabytes",
        "Terabytes",
        "Bits",
        "Kilobits",
        "Megabits",
        "Gigabits",
        "Terabits",
        "Percent",
        "Count",
        "Bytes/Second",
        "Kilobytes/Second",
        "Megabytes/Second",
        "Gigabytes/Second",
        "Terabytes/Second",
        "Bits/Second",
        "Kilobits/Second",
        "Megabits/Second",
        "Gigabits/Second",
        "Terabits/Second",
        "Count/Second",
        "None",
    ]
)


class MetricStatistics(TypedDict):
    SampleCount: float
    Sum: float
    Minimum: float
    Maximum: float


class Dimension(TypedDict):
    Name: str
    Value: str


class _MetricData(TypedDict):
    MetricName: str


class MetricData(_MetricData, total=False):
    Dimensions: ty.List[Dimension]
    Value: float
    Values: ty.List[float]
    StatisticValues: MetricStatistics
    Unit: str
    Counts: ty.List[float]
    StorageResolution: int
    Timestamp: int  # ms since unix epoch


def metric_data_maker(
    metric_name: str,
    *,
    dimensions: ty.Sequence[Dimension] = tuple(),
    unit: str = "Count",
    storage_resolution: int = 60,
) -> ty.Callable[..., MetricData]:
    def make_metric_data(
        value: float = 0.0,
        values: ty.Sequence[float] = tuple(),
        counts: ty.Sequence[float] = tuple(),
        statistic_values: ty.Optional[MetricStatistics] = None,
        timestamp: ty.Optional[ty.Union[int, datetime]] = None,
    ) -> MetricData:
        """If values, counts, or statistic_values are provided, 'value' will be ignored"""
        metric_data: MetricData = ty.cast(MetricData, dict(Values=values, Counts=counts))
        if not values:
            metric_data.pop("Values")
        if not counts:
            metric_data.pop("Counts")
        if not metric_data:  # if we're still empty, put a value in here!
            metric_data["Value"] = value
        if statistic_values:
            metric_data["StatisticValues"] = statistic_values

        if dimensions:
            metric_data["Dimensions"] = list(dimensions)
        if timestamp is not None:
            if isinstance(timestamp, datetime):
                timestamp = int(timestamp.timestamp() * 1000)
            metric_data["Timestamp"] = timestamp
        metric_data["Unit"] = unit
        metric_data["StorageResolution"] = storage_resolution
        metric_data["MetricName"] = metric_name

        return metric_data

    return make_metric_data


class MetricPutter:
    """Will not be further developed since it embeds I/O."""

    def __init__(
        self,
        namespace: str,
        metric_name: str,
        *,
        dimensions: ty.Sequence[Dimension] = tuple(),
        unit: str = "Count",
        storage_resolution: int = 60,
    ):
        self.namespace = namespace
        self.metric_maker = metric_data_maker(
            metric_name, dimensions=dimensions, unit=unit, storage_resolution=storage_resolution
        )

    def __call__(
        self,
        value: float = 0.0,
        values: ty.Sequence[float] = tuple(),
        counts: ty.Sequence[float] = tuple(),
        statistic_values: ty.Optional[MetricStatistics] = None,
        timestamp: ty.Optional[datetime] = None,
    ):
        """If values, counts, or statistic_values are provided, 'value' will be ignored"""
        metric_dict = dict(
            Namespace=self.namespace,
            MetricData=[self.metric_maker(value, values, counts, statistic_values, timestamp)],
        )
        logger.debug("put_metric", extra=dict(put_metric=metric_dict))
        CLOUDWATCH_CLIENT().put_metric_data(**metric_dict)  # type: ignore


PutMetricReturner = ty.Callable[..., float]
PutMetricDecorator = ty.Callable[[PutMetricReturner], PutMetricReturner]


def make_metric_putter_decorator(metric_putter: MetricPutter) -> PutMetricDecorator:
    """This is a fairly nice way of making a decorator that you can then use
    to wrap functions that return a value that you'd like to have sent as a CloudWatch metric.
    """

    def decorator(f):
        @functools.wraps(f)
        def put_metric_returner_wrapper(*args, **kwargs) -> float:
            """The float returned is the value for the metric"""
            value = f(*args, **kwargs)

            try:
                metric_putter(value)
            except botocore.exceptions.ClientError as ce:
                # don't fail something else just because we couldn't send this metric to CloudWatch
                logger.error(ce)
            return value

        return put_metric_returner_wrapper

    return decorator


def make_put_metric_decorator(
    namespace: str, metric_name: str, **metric_putter_kwargs
) -> PutMetricDecorator:
    metric_putter = MetricPutter(namespace, metric_name, **metric_putter_kwargs)
    return make_metric_putter_decorator(metric_putter)
