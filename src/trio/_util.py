from __future__ import annotations

import collections.abc
import inspect
import signal
from abc import ABCMeta
from collections.abc import Awaitable, Callable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    NoReturn,
    TypeVar,
    final as std_final,
)

from sniffio import thread_local as sniffio_loop

import trio

CallT = TypeVar("CallT", bound=Callable[..., Any])  # type: ignore[explicit-any]
T = TypeVar("T")
RetT = TypeVar("RetT")

if TYPE_CHECKING:
    import sys
    from types import AsyncGeneratorType, TracebackType

    from typing_extensions import TypeVarTuple, Unpack

    if sys.version_info < (3, 11):
        from exceptiongroup import BaseExceptionGroup

    PosArgsT = TypeVarTuple("PosArgsT")


def is_main_thread() -> bool:
    """Attempt to reliably check if we are in the main thread."""
    try:
        signal.signal(signal.SIGINT, signal.getsignal(signal.SIGINT))
        return True
    except (TypeError, ValueError):
        return False




class ConflictDetector:

    def __init__(self, msg: str) -> None:
        self._msg = msg
        self._held = False

    def __enter__(self) -> None:
        if self._held:
            raise trio.BusyResourceError(self._msg)
        else:
            self._held = True

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._held = False


def async_wraps(  # type: ignore[explicit-any]
    cls: type[object],
    wrapped_cls: type[object],
    attr_name: str,
) -> Callable[[CallT], CallT]:
    pass


def fixup_module_metadata(
    module_name: str,
    namespace: collections.abc.Mapping[str, object],
) -> None:
    seen_ids: set[int] = set()

    def fix_one(qualname: str, name: str, obj: object) -> None:
        if id(obj) in seen_ids:
            return
        seen_ids.add(id(obj))

        mod = getattr(obj, "__module__", None)
        if mod is not None and mod.startswith("trio."):
            obj.__module__ = module_name
            if hasattr(obj, "__name__") and "." not in obj.__name__:
                obj.__name__ = name
                if hasattr(obj, "__qualname__"):
                    obj.__qualname__ = qualname
            if isinstance(obj, type):
                for attr_name, attr_value in obj.__dict__.items():
                    fix_one(objname + "." + attr_name, attr_name, attr_value)

    for objname, obj in namespace.items():
        if not objname.startswith("_"):  # ignore private attributes
            fix_one(objname, objname, obj)


def _init_final_cls(cls: type[object]) -> NoReturn:
    """Raises an exception when a final class is subclassed."""
    raise TypeError(f"{cls.__module__}.{cls.__qualname__} does not support subclassing")


def _final_impl(decorated: type[T]) -> type[T]:
    pass


if TYPE_CHECKING:
    from typing import final
else:
    final = _final_impl


@final  # No subclassing of NoPublicConstructor itself.
class NoPublicConstructor(ABCMeta):

    def __call__(cls, *args: object, **kwargs: object) -> None:
        raise TypeError(
            f"{cls.__module__}.{cls.__qualname__} has no public constructor",
        )

    def _create(cls: type[T], *args: object, **kwargs: object) -> T:
        return super().__call__(*args, **kwargs)  # type: ignore


def name_asyncgen(agen: AsyncGeneratorType[object, NoReturn]) -> str:
    """Return the fully-qualified name of the async generator function
    that produced the async generator iterator *agen*.
    """
    if not hasattr(agen, "ag_code"):  # pragma: no cover
        return repr(agen)
    try:
        module = agen.ag_frame.f_globals["__name__"]  # type: ignore[union-attr]
    except (AttributeError, KeyError):
        module = f"<{agen.ag_code.co_filename}>"
    try:
        qualname = agen.__qualname__
    except AttributeError:
        qualname = agen.ag_code.co_name
    return f"{module}.{qualname}"


if TYPE_CHECKING:
    Fn = TypeVar("Fn", bound=Callable[..., object])  # type: ignore[explicit-any]

    def wraps(  # type: ignore[explicit-any]
        wrapped: Callable[..., object],
        assigned: Sequence[str] = ...,
        updated: Sequence[str] = ...,
    ) -> Callable[[Fn], Fn]: ...

else:
    from functools import wraps  # noqa: F401  # this is re-exported


def raise_saving_context(exc: BaseException) -> NoReturn:
    """This helper allows re-raising an exception without __context__ being set."""
    __tracebackhide__ = True
    context = exc.__context__
    try:
        raise exc
    finally:
        exc.__context__ = context
        del exc, context


class MultipleExceptionError(Exception):
    pass


def raise_single_exception_from_group(
    eg: BaseExceptionGroup[BaseException],
) -> NoReturn:
    """This function takes an exception group that is assumed to have at most
    one non-cancelled exception, which it reraises as a standalone exception.

    This exception may be an exceptiongroup itself, in which case it will not be unwrapped.

    If a :exc:`KeyboardInterrupt` is encountered, a new KeyboardInterrupt is immediately
    raised with the entire group as cause.

    If the group only contains :exc:`Cancelled` it reraises the first one encountered.

    It will retain context and cause of the contained exception, and entirely discard
    the cause/context of the group(s).

    If multiple non-cancelled exceptions are encountered, it raises
    :exc:`AssertionError`.
    """
    for e in eg.exceptions:
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            raise type(e)(*e.args) from eg

    cancelled_exception: trio.Cancelled | None = None
    noncancelled_exception: BaseException | None = None

    for e in eg.exceptions:
        if isinstance(e, trio.Cancelled):
            if cancelled_exception is None:
                cancelled_exception = e
        elif noncancelled_exception is None:
            noncancelled_exception = e
        else:
            raise MultipleExceptionError(
                "Attempted to unwrap exceptiongroup with multiple non-cancelled exceptions. This is often caused by a bug in the caller."
            ) from eg

    if noncancelled_exception is not None:
        raise_saving_context(noncancelled_exception)

    assert cancelled_exception is not None, "group can't be empty"
    raise_saving_context(cancelled_exception)
