"""A utility supporting a nicer API for basic SSM parameter store
operations, which also transparently supports splitting large payloads
across multiple parameters and joining them upon get using this same
utility.
"""
import re
import os
import os.path
import json
from datetime import datetime

from botocore.exceptions import ClientError

from xoto3.lazy import tlls


_MULTIPART_PARAM_COUNT = "__SSM_MULTIPART_COUNT"

_SSM_NOT_ALLOWED_REGEX = re.compile("[^0-9a-zA-Z_.-]+")

try:
    import __main__ as main_func
    SCRIPT_NAME = os.path.basename(main_func.__file__)
except Exception:  # pylint: disable=broad-except  # this is okay because getting the name is best-effort
    SCRIPT_NAME = "SSM Params"


SSM_CLIENT = tlls("client", "ssm")


def delete(name: str, ssm=None) -> bool:
    """Returns True if parameter was deleted,
    False if it didn't exist,
    and raises for other errors"""
    ssm = ssm or SSM_CLIENT()
    try:
        ssm.delete_parameter(Name=name)
    except ClientError as ce:
        if _get_client_error_type(ce) == "ParameterNotFound":
            return True
        raise ce
    return True


def get(name: str, default=None, error=True, ssm=None, **kwargs) -> str:
    """General utility for getting SSM parameter values.

    Returns only the actual value, not the full boto3 response.

    Handles large/multipart gets transparently.

    Providing a default or setting error=False will only prevent
    ParameterNotFound from raising an exception - all other boto3
    ClientErrors will always raise.
    """
    ssm = ssm or SSM_CLIENT()

    try:
        param = ssm.get_parameter(Name=name, **kwargs)
    except ClientError as ce:
        if _get_client_error_type(ce) != "ParameterNotFound":
            raise ce  # always raise if we've having unusual difficulties
        if default is not None or not error:
            return default
        raise ce
    param_value = param["Parameter"]["Value"]

    # check if this is a multipart param
    param_part_count = _get_num_multiparts(param_value)
    if param_part_count:
        try:
            return _get_multipart_param(name, param_part_count, ssm)
        except (KeyError, ValueError, ClientError) as e:
            raise RuntimeError(f"SSM multipart param {name} is not complete", e)
    return param_value


def put(Name: str, Value: str, Type="String", Overwrite=True, ssm=None, **kwargs):
    """General utility for putting SSM parameter values.

    Handles large/multipart puts transparently.

    Raises all ClientErrors to the caller.
    """
    ssm = ssm or SSM_CLIENT()

    if len(Value) > 4096:
        _put_multipart_param(Name, Value, ssm, Type)
    else:
        if "Description" not in kwargs:
            kwargs["Description"] = f"Set by {SCRIPT_NAME} at " + datetime.now().isoformat()
        ssm.put_parameter(Name=Name, Value=Value, Type=Type, Overwrite=Overwrite, **kwargs)


def ssm_allowed_name(unsafe_name: str) -> str:
    """Replace characters that aren't allowed as part of an SSM name"""
    return _SSM_NOT_ALLOWED_REGEX.sub("_", unsafe_name)


def _get_client_error_type(clienterror: ClientError):
    return clienterror.response.get("Error", {}).get("Code")


def _get_num_multiparts(param_value: str) -> int:
    """Returns 0 if not multipart, or more than 1 if multipart"""
    try:
        param_obj = json.loads(param_value)
        if isinstance(param_obj, dict) and _MULTIPART_PARAM_COUNT in param_obj:
            return int(param_obj[_MULTIPART_PARAM_COUNT])
        return 0
    except ValueError:
        return 0


def _get_multipart_param(param_name: str, count: int, ssm) -> str:
    """You must pass the count returned from _get_num_multiparts"""
    param_value = ""
    i = 0
    for i in range(count):
        param_value += ssm.get_parameter(Name=_get_multipart_param_part_name(param_name, i))[
            "Parameter"
        ]["Value"]
    return param_value


def _get_multipart_param_part_name(name, part_num):
    return f"{name}__{part_num}"


def div_ceil(a, b):
    val = -(-a // b)
    assert val * b >= a
    return val


def _delete_multipart_param(name, ssm, count=None):
    if not count:
        # find out how many there are
        count = _get_num_multiparts(ssm.get_parameter(Name=name))
    for i in range(count):
        try:
            ssm.delete_parameter(Name=_get_multipart_param_part_name(name, i))
        except ClientError as ce:
            if _get_client_error_type(ce) != "ParameterNotFound":
                raise ce  # indicates a more serious problem than just a missing piece


def _put_multipart_param(name, value, ssm, Type):
    num_parts = div_ceil(len(value), 4096)
    thistime = datetime.now().isoformat()

    put_param_payload = dict(
        Name=name,
        Value=json.dumps({_MULTIPART_PARAM_COUNT: num_parts}),
        Description="A multipart SSM parameter, stored at " + thistime + f", by {SCRIPT_NAME}",
        Overwrite=False,
        Type=Type,
    )
    try:
        ssm.put_parameter(**put_param_payload)
    except ClientError as ce:
        if _get_client_error_type(ce) != "ParameterAlreadyExists":
            raise ce
        existing_num_parts = _get_num_multiparts(ssm.get_parameter(Name=name)["Parameter"]["Value"])
        if existing_num_parts > num_parts:
            # clean up existing parameter so we don't have 'leftovers'
            _delete_multipart_param(name, ssm, count=existing_num_parts)
        put_param_payload["Overwrite"] = True
        ssm.put_parameter(**put_param_payload)

    for i in range(num_parts):
        des = f"Part {i+1} of {num_parts} of {name}, stored at " + thistime + f", by {SCRIPT_NAME}"
        ssm.put_parameter(
            Name=_get_multipart_param_part_name(name, i),
            Value=value[i * 4096 : (i + 1) * 4096],
            Description=des,
            Overwrite=True,
            Type=Type,
        )
