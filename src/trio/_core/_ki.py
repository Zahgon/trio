from __future__ import annotations

import signal
import sys
import weakref
from typing import TYPE_CHECKING, Generic, Protocol, TypeGuard, TypeVar

import attrs

from .._util import is_main_thread
from ._run_context import GLOBAL_RUN_CONTEXT

if TYPE_CHECKING:
    import types
    from collections.abc import Callable

    from typing_extensions import Self

_T = TypeVar("_T")


class _IdRef(weakref.ref[_T]):
    __slots__ = ("_hash",)
    _hash: int

    def __new__(
        cls,
        ob: _T,
        callback: Callable[[Self], object] | None = None,
        /,
    ) -> Self:
        self: Self = weakref.ref.__new__(cls, ob, callback)
        self._hash = object.__hash__(ob)
        return self

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True

        if not isinstance(other, _IdRef):
            return NotImplemented

        my_obj = None
        try:
            my_obj = self()
            return my_obj is not None and my_obj is other()
        finally:
            del my_obj

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return self._hash


_KT = TypeVar("_KT")
_VT = TypeVar("_VT")


class WeakKeyIdentityDictionary(Generic[_KT, _VT]):
    def __init__(self) -> None:
        self._data: dict[_IdRef[_KT], _VT] = {}

        def remove(
            k: _IdRef[_KT],
            selfref: weakref.ref[
                WeakKeyIdentityDictionary[_KT, _VT]
            ] = weakref.ref(  # noqa: B008  # function-call-in-default-argument
                self,
            ),
        ) -> None:
            self = selfref()
            if self is not None:
                try:  # noqa: SIM105  # suppressible-exception
                    del self._data[k]
                except KeyError:
                    pass

        self._remove = remove

    def __getitem__(self, k: _KT) -> _VT:
        return self._data[_IdRef(k)]

    def __setitem__(self, k: _KT, v: _VT) -> None:
        self._data[_IdRef(k, self._remove)] = v


_CODE_KI_PROTECTION_STATUS_WMAP: WeakKeyIdentityDictionary[
    types.CodeType,
    bool,
] = WeakKeyIdentityDictionary()




def ki_protection_enabled(frame: types.FrameType | None) -> bool:
    try:
        task = GLOBAL_RUN_CONTEXT.task
    except AttributeError:
        task_ki_protected = False
        task_frame = None
    else:
        task_ki_protected = task._ki_protected
        task_frame = task.coro.cr_frame

    while frame is not None:
        try:
            v = _CODE_KI_PROTECTION_STATUS_WMAP[frame.f_code]
        except KeyError:
            pass
        else:
            return bool(v)
        if frame.f_code.co_name == "__del__":
            return True
        if frame is task_frame:
            return task_ki_protected
        frame = frame.f_back
    return True


def currently_ki_protected() -> bool:
    r"""Check whether the calling code has :exc:`KeyboardInterrupt` protection
    enabled.

    It's surprisingly easy to think that one's :exc:`KeyboardInterrupt`
    protection is enabled when it isn't, or vice-versa. This function tells
    you what Trio thinks of the matter, which makes it useful for ``assert``\s
    and unit tests.

    Returns:
      bool: True if protection is enabled, and False otherwise.

    """
    return ki_protection_enabled(sys._getframe())


class _SupportsCode(Protocol):
    __code__: types.CodeType


_T_supports_code = TypeVar("_T_supports_code", bound=_SupportsCode)


def enable_ki_protection(f: _T_supports_code, /) -> _T_supports_code:
    pass


def disable_ki_protection(f: _T_supports_code, /) -> _T_supports_code:
    pass


@attrs.define(slots=False)
class KIManager:
    handler: Callable[[int, types.FrameType | None], None] | None = None

    def install(
        self,
        deliver_cb: Callable[[], object],
        restrict_keyboard_interrupt_to_checkpoints: bool,
    ) -> None:
        assert self.handler is None
        if (
            not is_main_thread()
            or signal.getsignal(signal.SIGINT) != signal.default_int_handler
        ):
            return


        self.handler = handler
        signal.signal(signal.SIGINT, handler)

    def close(self) -> None:
        if self.handler is not None:
            if signal.getsignal(signal.SIGINT) is self.handler:
                signal.signal(signal.SIGINT, signal.default_int_handler)
            self.handler = None
