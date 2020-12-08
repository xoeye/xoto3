from xoto3.cloudwatch.metrics import metric_data_maker


def test_basic_metric_maker():
    agg = metric_data_maker(
        "test_metric", dimensions=[dict(Name="FunctionName", Value="outbound")], unit="Seconds",
    )

    assert agg(4.3) == dict(
        MetricName="test_metric",
        Dimensions=[dict(Name="FunctionName", Value="outbound")],
        Unit="Seconds",
        StorageResolution=60,
        Value=4.3,
    )
