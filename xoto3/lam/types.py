"""Types for AWS things"""
import typing as ty
from typing_extensions import Protocol


class LambdaContext(Protocol):
    """This is what a Lambda Context looks like in Python"""

    function_name: str
    function_version: str
    aws_request_id: str
    invoked_function_arn: str
    log_group_name: str
    log_stream_name: str
    memory_limit_in_mb: str
    _epoch_deadline_time_in_ms: int


Event = ty.Dict[str, ty.Any]

# return value of a lambda must be JSON-serializable or None
LambdaEntryPoint = ty.Callable[[Event, LambdaContext], ty.Any]

LambdaEntryPointDecorator = ty.Callable[[LambdaEntryPoint], LambdaEntryPoint]
