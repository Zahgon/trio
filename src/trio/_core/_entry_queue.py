from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, NoReturn

import attrs

from .. import _core
from .._util import NoPublicConstructor, final
from ._wakeup_socketpair import WakeupSocketpair

if TYPE_CHECKING:
    from typing_extensions import TypeVarTuple, Unpack

    PosArgsT = TypeVarTuple("PosArgsT")

Function = Callable[..., object]  # type: ignore[explicit-any]
Job = tuple[Function, tuple[object, ...]]


@attrs.define
class EntryQueue:
    queue: deque[Job] = attrs.Factory(deque)
    idempotent_queue: dict[Job, None] = attrs.Factory(dict)

    wakeup: WakeupSocketpair = attrs.Factory(WakeupSocketpair)
    done: bool = False
    lock: threading.RLock = attrs.Factory(threading.RLock)


    def close(self) -> None:
        self.wakeup.close()

    def size(self) -> int:
        return len(self.queue) + len(self.idempotent_queue)

    def run_sync_soon(
        self,
        sync_fn: Callable[[Unpack[PosArgsT]], object],
        *args: Unpack[PosArgsT],
        idempotent: bool = False,
    ) -> None:
        with self.lock:
            if self.done:
                raise _core.RunFinishedError("run() has exited")
            if idempotent:
                self.idempotent_queue[sync_fn, args] = None
            else:
                self.queue.append((sync_fn, args))
            self.wakeup.wakeup_thread_and_signal_safe()


@final
@attrs.define(eq=False)
class TrioToken(metaclass=NoPublicConstructor):

    _reentry_queue: EntryQueue

    def run_sync_soon(
        self,
        sync_fn: Callable[[Unpack[PosArgsT]], object],
        *args: Unpack[PosArgsT],
        idempotent: bool = False,
    ) -> None:
        """Schedule a call to ``sync_fn(*args)`` to occur in the context of a
        Trio task.

        This is safe to call from the main thread, from other threads, and
        from signal handlers. This is the fundamental primitive used to
        re-enter the Trio run loop from outside of it.

        The call will happen "soon", but there's no guarantee about exactly
        when, and no mechanism provided for finding out when it's happened.
        If you need this, you'll have to build your own.

        The call is effectively run as part of a system task (see
        :func:`~trio.lowlevel.spawn_system_task`). In particular this means
        that:

        * :exc:`KeyboardInterrupt` protection is *enabled* by default; if
          you want ``sync_fn`` to be interruptible by control-C, then you
          need to use :func:`~trio.lowlevel.disable_ki_protection`
          explicitly.

        * If ``sync_fn`` raises an exception, then it's converted into a
          :exc:`~trio.TrioInternalError` and *all* tasks are cancelled. You
          should be careful that ``sync_fn`` doesn't crash.

        All calls with ``idempotent=False`` are processed in strict
        first-in first-out order.

        If ``idempotent=True``, then ``sync_fn`` and ``args`` must be
        hashable, and Trio will make a best-effort attempt to discard any
        call submission which is equal to an already-pending call. Trio
        will process these in first-in first-out order.

        Any ordering guarantees apply separately to ``idempotent=False``
        and ``idempotent=True`` calls; there's no rule for how calls in the
        different categories are ordered with respect to each other.

        :raises trio.RunFinishedError:
              if the associated call to :func:`trio.run`
              has already exited. (Any call that *doesn't* raise this error
              is guaranteed to be fully processed before :func:`trio.run`
              exits.)

        """
        self._reentry_queue.run_sync_soon(sync_fn, *args, idempotent=idempotent)
