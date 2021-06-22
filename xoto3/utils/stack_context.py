import contextlib as cl
import contextvars as cv
import typing as ty

T = ty.TypeVar("T")
F = ty.TypeVar("F", bound=ty.Callable)


@cl.contextmanager
def stack_context(contextvar: cv.ContextVar[T], value: T) -> ty.Iterator:
    """Provide context down the stack without 'parameter drilling'.

    A ContextManager for the value of a ContextVar.

    Sometimes you need to be able to push arguments through layers of
    function calls without polluting the signature of every function
    in between. We all know globals are a bad idea, the DX of thread
    locals is not much better, and doesn't work with async code. This
    uses ContextVar under the hood (to be compatible with async) and
    also makes what you're doing a lot more explicit.

    Caveat emptor: This is essentially functional dependency
    injection. Like all forms of dependency injection, it creates
    action at a distance. The advantage is that you can reduce
    coupling in intermediate layers. The disadvantage is that even
    though this is more explicit than globals, it's still less
    explicit than parameter drilling.

    So, In some cases it may be better for your code to make its
    dependencies explicit by going ahead and doing prop-drilling, so
    don't use this just because you can. In many cases, a functional
    core, imperative shell approach will be clearer. This is for cases
    where the benefits of the magic clearly outweigh its harms. Choose
    wisely.

    """
    try:
        token = contextvar.set(value)
        yield
    finally:
        contextvar.reset(token)


class StackContext(ty.Generic[T]):
    """A thin wrapper around a ContextVar that requires it to be set in a
    stack-frame limited manner.

    These should only be created at a module level, just like the
    underlying ContextVar.

    """

    def __init__(self, name: str, default: T):
        self._contextvar = cv.ContextVar(name, default=default)

    def set(self, value: T) -> ty.ContextManager[T]:
        return stack_context(self._contextvar, value)

    def get(self) -> T:
        return self._contextvar.get()

    def __call__(self) -> T:
        """4 fewer characters than .get()"""
        return self.get()


def unwrap(callable: ty.Callable[[], ty.Callable[[], T]], *, layers: int = 1) -> ty.Callable[[], T]:
    """It's pretty common to want to provide a Callable that
    requires no arguments and returns a value, e.g. so that your
    default value can be lazily loaded.

    This just gives you a way of saying that you always want the
    wrapped value whenever you call the outer callable.

    """

    def unwrapping() -> T:
        c = callable
        for i in range(layers):
            c = c()  # type: ignore
        return c()  # type: ignore

    return unwrapping
