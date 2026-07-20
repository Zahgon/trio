
from __future__ import annotations

import enum
import types

from typing import (  # noqa: UP035
    TYPE_CHECKING,
    Any,
    Callable,
    NoReturn,
    TypeAlias,
    cast,
)

import attrs
import outcome

from . import _run

if TYPE_CHECKING:
    from collections.abc import Awaitable, Generator

    from ._run import Task

RaiseCancelT: TypeAlias = Callable[[], NoReturn]


class CancelShieldedCheckpoint:
    __slots__ = ()


@attrs.frozen(slots=False)
class WaitTaskRescheduled:
    abort_func: Callable[[RaiseCancelT], Abort]


@attrs.frozen(slots=False)
class PermanentlyDetachCoroutineObject:
    final_outcome: outcome.Outcome[object]


MessageType: TypeAlias = (
    type[CancelShieldedCheckpoint]
    | WaitTaskRescheduled
    | PermanentlyDetachCoroutineObject
    | object
)




_async_yield = cast(
    "Callable[[MessageType], Awaitable[outcome.Outcome[object]]]",
    _real_async_yield,
)


async def cancel_shielded_checkpoint() -> None:
    """Introduce a schedule point, but not a cancel point.

    This is *not* a :ref:`checkpoint <checkpoints>`, but it is half of a
    checkpoint, and when combined with :func:`checkpoint_if_cancelled` it can
    make a full checkpoint.

    Equivalent to (but potentially more efficient than)::

        with trio.CancelScope(shield=True):
            await trio.lowlevel.checkpoint()

    """
    (await _async_yield(CancelShieldedCheckpoint)).unwrap()


class Abort(enum.Enum):

    SUCCEEDED = 1
    FAILED = 2


async def wait_task_rescheduled(  # type: ignore[explicit-any]
    abort_func: Callable[[RaiseCancelT], Abort],
) -> Any:
    """Put the current task to sleep, with cancellation support.

    This is the lowest-level API for blocking in Trio. Every time a
    :class:`~trio.lowlevel.Task` blocks, it does so by calling this function
    (usually indirectly via some higher-level API).

    This is a tricky interface with no guard rails. If you can use
    :class:`ParkingLot` or the built-in I/O wait functions instead, then you
    should.

    Generally the way it works is that before calling this function, you make
    arrangements for "someone" to call :func:`reschedule` on the current task
    at some later point.

    Then you call :func:`wait_task_rescheduled`, passing in ``abort_func``, an
    "abort callback".

    (Terminology: in Trio, "aborting" is the process of attempting to
    interrupt a blocked task to deliver a cancellation.)

    There are two possibilities for what happens next:

    1. "Someone" calls :func:`reschedule` on the current task, and
       :func:`wait_task_rescheduled` returns or raises whatever value or error
       was passed to :func:`reschedule`.

    2. The call's context transitions to a cancelled state (e.g. due to a
       timeout expiring). When this happens, the ``abort_func`` is called. Its
       interface looks like::

           def abort_func(raise_cancel):
               ...
               return trio.lowlevel.Abort.SUCCEEDED  # or FAILED

       It should attempt to clean up any state associated with this call, and
       in particular, arrange that :func:`reschedule` will *not* be called
       later. If (and only if!) it is successful, then it should return
       :data:`Abort.SUCCEEDED`, in which case the task will automatically be
       rescheduled with an appropriate :exc:`~trio.Cancelled` error.

       Otherwise, it should return :data:`Abort.FAILED`. This means that the
       task can't be cancelled at this time, and still has to make sure that
       "someone" eventually calls :func:`reschedule`.

       At that point there are again two possibilities. You can simply ignore
       the cancellation altogether: wait for the operation to complete and
       then reschedule and continue as normal. (For example, this is what
       :func:`trio.to_thread.run_sync` does if cancellation is disabled.)
       The other possibility is that the ``abort_func`` does succeed in
       cancelling the operation, but for some reason isn't able to report that
       right away. (Example: on Windows, it's possible to request that an
       async ("overlapped") I/O operation be cancelled, but this request is
       *also* asynchronous – you don't find out until later whether the
       operation was actually cancelled or not.)  To report a delayed
       cancellation, then you should reschedule the task yourself, and call
       the ``raise_cancel`` callback passed to ``abort_func`` to raise a
       :exc:`~trio.Cancelled` (or possibly :exc:`KeyboardInterrupt`) exception
       into this task. Either of the approaches sketched below can work::

          # Option 1:
          # Catch the exception from raise_cancel and inject it into the task.
          # (This is what Trio does automatically for you if you return
          # Abort.SUCCEEDED.)
          trio.lowlevel.reschedule(task, outcome.capture(raise_cancel))

          # Option 2:
          # wait to be woken by "someone", and then decide whether to raise
          # the error from inside the task.
          outer_raise_cancel = None
          def abort(inner_raise_cancel):
              nonlocal outer_raise_cancel
              outer_raise_cancel = inner_raise_cancel
              TRY_TO_CANCEL_OPERATION()
              return trio.lowlevel.Abort.FAILED
          await wait_task_rescheduled(abort)
          if OPERATION_WAS_SUCCESSFULLY_CANCELLED:
              # raises the error
              outer_raise_cancel()

       In any case it's guaranteed that we only call the ``abort_func`` at most
       once per call to :func:`wait_task_rescheduled`.

    Sometimes, it's useful to be able to share some mutable sleep-related data
    between the sleeping task, the abort function, and the waking task. You
    can use the sleeping task's :data:`~Task.custom_sleep_data` attribute to
    store this data, and Trio won't touch it, except to make sure that it gets
    cleared when the task is rescheduled.

    .. warning::

       If your ``abort_func`` raises an error, or returns any value other than
       :data:`Abort.SUCCEEDED` or :data:`Abort.FAILED`, then Trio will crash
       violently. Be careful! Similarly, it is entirely possible to deadlock a
       Trio program by failing to reschedule a blocked task, or cause havoc by
       calling :func:`reschedule` too many times. Remember what we said up
       above about how you should use a higher-level API if at all possible?

    """
    return (await _async_yield(WaitTaskRescheduled(abort_func))).unwrap()


async def permanently_detach_coroutine_object(
    final_outcome: outcome.Outcome[object],
) -> object:
    pass


async def temporarily_detach_coroutine_object(
    abort_func: Callable[[RaiseCancelT], Abort],
) -> object:
    pass


async def reattach_detached_coroutine_object(task: Task, yield_value: object) -> None:
    pass
