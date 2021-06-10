"""A mechanism for providing less-verbose dynamic defaults for functions"""

import inspect
import typing as ty
from functools import wraps

F = ty.TypeVar("F", bound=ty.Callable)
T = ty.TypeVar("T")


class NotSafeToDefaultError(ValueError):
    """A positional parameter without a default is not safe to provide a
    default for, because it is not logical for a caller to expect not
    to have to provide an argument, and if there is more than one
    positional argument provided by the caller at runtime, it will be
    impossible for us to know which one the caller actually meant to
    provide, leading to ambiguous errors.

    A keyword-only argument without a default is allowed only because
    the callers will always have to specify it by name. In practice,
    it's a bad idea to apply dynamic defaults to a keyword argument
    with no default, because it will look very confusing to IDEs, type
    checkers, etc.

    A function with a var-kwargs collector (**kw) will also be allowed
    if the parameter name is not found in the function signature. That
    dictionary will always have your default populated unless the
    caller supplied one.

    Any other type of argument is not dynamically defaultable and this
    error will be raised.

    """


def _validate_argument_is_defaultable(f: F, param_name: str) -> float:
    """Returns float infinity if the parameter is keyword-only, and
    returns the parameter position if the argument can be supplied
    positionally but a default is defined, meaning that it is past the
    positional arguments that don't have a default, which means that a
    caller not supplying this argument will also not be supplying any
    other positional arguments positionally.

    """
    sig = inspect.signature(f)
    has_var_kwargs = False
    for i, param in enumerate(sig.parameters.values()):
        if param.name == param_name:
            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if param.default is inspect.Parameter.empty:
                    raise NotSafeToDefaultError(
                        f"Positional function parameter {param_name} must have a default "
                        "to be eligible for having an oncall default provided."
                    )
                return i
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                # allow **kwargs to have a mergeable default set
                return -1
            if param.kind == inspect.Parameter.KEYWORD_ONLY:
                # if it can only be supplied by keyword, then it is
                # always simple to identify whether the caller
                # provided it.
                return float("inf")
            raise NotSafeToDefaultError(
                f"Cannot create a decorated function where {param_name} "
                "is present but not one of the matched types"
            )
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            has_var_kwargs = True
    # the argument was not found by name. If var kwargs were defined,
    # then we simply provide a default that way. Otherwise, this cannot work
    if has_var_kwargs:
        return float("inf")
    raise NotSafeToDefaultError(
        f"Could not find function parameter {param_name} "
        "and there is no variable keyword arguments parameter defined"
    )


def make_oncall_default_deco(
    default_callable: ty.Callable[[], T], param_name: str
) -> ty.Callable[[F], F]:
    def oncall_default_param_decorator(f: F) -> F:
        pos_num = _validate_argument_is_defaultable(f, param_name)

        @wraps(f)
        def wrapper(*args, **kwargs):
            if pos_num == -1:
                # merge default kwargs with provided kwargs
                default_kwargs = default_callable()
                assert isinstance(
                    default_kwargs, ty.Mapping
                ), "A default for kwargs itself must be a mapping so it can be merged with other keyword arguments"
                kwargs = dict(default_kwargs, **kwargs)
            elif param_name not in kwargs:
                # if it was provided as a keyword argument, then we shouldn't override it
                if len(args) <= pos_num:
                    # the argument is keyword-only or was not provided as a positional argument
                    kwargs[param_name] = default_callable()
            return f(*args, **kwargs)

        return ty.cast(F, wrapper)

    return oncall_default_param_decorator


class OnCallDefault(ty.Generic[T]):
    """Creates a partially-applied decorator to specify that you want a
    oncall default value for a safely-defaultable function parameter.

    Python's default behavior is to define a static default when the
    function is defined.  In some cases, this is not at all what you
    want - for instance, you might want an empty, mutable list every
    time the function is called, but Python will not allow you to
    define that. The decorator created here will allow you to specify
    what callable you want to use to create your default, and will
    inject its result into every function call where no value has been
    provided by the caller.

    This only works for certain cases:

    1. Keyword-only parameters, preferably with a defined default.

    2. Arguments provided to the function via var-kwargs (**kwargs) capture.

    3. Positional-or-keyword parameters where a default is explicitly
       provided in the function definition.

    4. The full **kwargs dictionary. This is a special case where we
       will take your provided default dictionary and then perform a
       shallow merge of the provided keyword arguments.

    The limitations are because it is not otherwise possible to
    identify which arguments a caller unaware of the special oncall
    default behavior _did not_ intend to provide solely by their
    position. By preserving this restriction, we allow callers to
    remain blissfully ignorant of the application of this decorator.

    PLEASE NOTE: It is strongly recommended that you use this only for
    keyword-only arguments that your function does not require to be
    provided. In other words, when someone reads your function
    signature without the decorator, they should be able to understand
    in what ways your function will be callable.  Usage with
    positional arguments in functions with complex signatures should
    be conisdered borderline sociopathic even if it works.

    ```
    utcnow = OnCallDefault(datetime.utcnow)

    @utcnow.default('updated_at')
    def do_the_thing(entity: E, new_bar: Bar, *, updated_at: datetime) -> E:
        entity.bar = new_bar
        entity.updated_at = updated_at
        return entity
    ```

    then call as:

    `do_the_thing(my_entity, new_bar)`

    And let this wrapper fill in the current value from the OnCallDefault.

    If you want IDEs and type checkers to be happy with the call, you
    should also define an unused standard default - its value will
    never get used as long as the decorator is applied. This class
    provides a simply callable syntax for providing a typed default
    that the Python runtime and standard utilities will be able to
    intepret just fine.

    ```
    @utcnow.default('updated_at')
    def do_the_thing(entity: E, new_bar: Bar, updated_at: datetime = utcnow()) -> E:
        ...
    ```

    """

    def __init__(self, default_callable: ty.Callable[[], T]):
        self.default_callable = default_callable

    def apply_to(self, param_name: str) -> ty.Callable[[F], F]:
        return make_oncall_default_deco(self.default_callable, param_name)

    def __call__(self) -> T:
        """Returns the value of the callable. A convenience for defining your default"""
        return self.default_callable()
