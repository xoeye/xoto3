import typing as ty

from .oncall_default import F, OnCallDefault, T
from .stack_context import StackContext


class ContextualDefault(ty.Generic[T]):
    """A shortcut for implementing simple StackContexts that are used only
    for OnCallDefaults on a particular function.

    Though this _can_ be shared across functions, the parameter name
    will have to be identical, and you should consider having separate
    ContextVars per function for sanity. If your functions are truly
    logically grouped, it might make more sense to write a class.
    """

    def __init__(self, param_name: str, default: T, context_prefix: str = ""):
        self.param_name = param_name
        self.stack_context = StackContext(context_prefix + param_name, default)
        self.oncall_default = OnCallDefault(self.stack_context)

    def __call__(self) -> T:
        return self.stack_context()

    def apply(self, f: F) -> F:
        return self.oncall_default.apply_to(self.param_name)(f)

    def set_default(self, default_value: T):
        return self.stack_context.set(default_value)
