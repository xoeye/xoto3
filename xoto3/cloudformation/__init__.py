"""Gets and caches things from Cloudformation"""
import typing as ty

from xoto3.lazy_session import tlls


_CF_RESOURCE = tlls("resource", "cloudformation")


_STACKS: ty.Dict[str, ty.Any] = dict()
_NAMED_OUTPUTS: ty.Dict[str, str] = dict()


def _get_cached_stack(stack_name: str):
    if stack_name not in _STACKS:
        _STACKS[stack_name] = _CF_RESOURCE().Stack(stack_name)
    return _STACKS[stack_name]


def get_stack_output(stack, name: str) -> str:
    """Get a named output directly from a CF stack"""
    for output in stack.outputs:
        if output["OutputKey"] == name:
            return output["OutputValue"]
    raise ValueError(f"No stack output with name {name} found in stack {stack}!")


def get_cached_stack_output(stack_name: str, output_name: str) -> str:
    """Includes caching, and assumes this is a staged stack name."""
    cache_name = stack_name + output_name
    if cache_name not in _NAMED_OUTPUTS:
        stack = _get_cached_stack(stack_name)
        _NAMED_OUTPUTS[cache_name] = get_stack_output(stack, output_name)
    return _NAMED_OUTPUTS[cache_name]
