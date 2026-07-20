from __future__ import annotations

import enum
import functools
import gc
import itertools
import random
import select
import sys
import warnings
from collections import deque
from contextlib import AbstractAsyncContextManager, contextmanager, suppress
from contextvars import copy_context
from heapq import heapify, heappop, heappush
from math import inf, isnan
from time import perf_counter
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    NoReturn,
    Protocol,
    cast,
    overload,
)

import attrs
import outcome
from outcome import Error, Outcome, Value, capture
from sniffio import thread_local as sniffio_library
from sortedcontainers import SortedDict

from .. import _core
from .._abc import Clock, Instrument
from .._deprecate import warn_deprecated
from .._util import NoPublicConstructor, coroutine_or_error, final, raise_saving_context
from ._asyncgens import AsyncGenerators
from ._concat_tb import concat_tb
from ._entry_queue import EntryQueue, TrioToken
from ._exceptions import (
    Cancelled,
    CancelReasonLiteral,
    RunFinishedError,
    TrioInternalError,
)
from ._instrumentation import Instruments
from ._ki import KIManager, enable_ki_protection
from ._parking_lot import GLOBAL_PARKING_LOT_BREAKER
from ._run_context import GLOBAL_RUN_CONTEXT as GLOBAL_RUN_CONTEXT
from ._thread_cache import start_thread_soon
from ._traps import (
    Abort,
    CancelShieldedCheckpoint,
    PermanentlyDetachCoroutineObject,
    WaitTaskRescheduled,
    cancel_shielded_checkpoint,
    wait_task_rescheduled,
)

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


if TYPE_CHECKING:
    import contextvars
    import types
    from collections.abc import (
        Awaitable,
        Callable,
        Generator,
        Iterator,
        Sequence,
    )
    from types import TracebackType

    from typing_extensions import Self, TypeVar, TypeVarTuple, Unpack

    PosArgT = TypeVarTuple("PosArgT")
    StatusT = TypeVar("StatusT", default=None)
    StatusT_contra = TypeVar("StatusT_contra", contravariant=True, default=None)
    BaseExcT = TypeVar("BaseExcT", bound=BaseException)
else:
    from typing import TypeVar

    StatusT = TypeVar("StatusT")
    StatusT_contra = TypeVar("StatusT_contra", contravariant=True)

RetT = TypeVar("RetT")


DEADLINE_HEAP_MIN_PRUNE_THRESHOLD: Final = 1000

_NO_SEND: Final[Outcome[object]] = cast("Outcome[object]", object())

NONSTRICT_EXCEPTIONGROUP_NOTE = 'This is a "loose" ExceptionGroup, and may be collapsed by Trio if it only contains one exception - typically after `Cancelled` has been stripped from it. Note this has consequences for exception handling, and strict_exception_groups=True is recommended.'


@final
class _NoStatus(metaclass=NoPublicConstructor):
    pass




_ALLOW_DETERMINISTIC_SCHEDULING: Final = False
_r = random.Random()




def _count_context_run_tb_frames() -> int:
    """Count implementation dependent traceback frames from Context.run()

    On CPython, Context.run() is implemented in C and doesn't show up in
    tracebacks. On PyPy, it is implemented in Python and adds 1 frame to
    tracebacks.

    Returns:
        int: Traceback frame count

    """


    ctx = copy_context()
    try:
        ctx.run(function_with_unique_name_xyzzy)
    except ZeroDivisionError as exc:
        tb = exc.__traceback__
        tb = tb.tb_next  # type: ignore[union-attr]
        count = 0
        while tb.tb_frame.f_code.co_name != "function_with_unique_name_xyzzy":  # type: ignore[union-attr]
            tb = tb.tb_next  # type: ignore[union-attr]
            count += 1
        return count
    else:  # pragma: no cover
        raise TrioInternalError(
            f"The purpose of {function_with_unique_name_xyzzy.__name__} is "
            "to raise a ZeroDivisionError, but it didn't.",
        )


CONTEXT_RUN_TB_FRAMES: Final = _count_context_run_tb_frames()


@attrs.frozen
class SystemClock(Clock):
    offset: float = attrs.Factory(lambda: _r.uniform(10000, 200000))

    def start_clock(self) -> None:
        pass

    def current_time(self) -> float:
        return self.offset + perf_counter()

    def deadline_to_sleep_time(self, deadline: float) -> float:
        return deadline - self.current_time()


class IdlePrimedTypes(enum.Enum):
    WAITING_FOR_IDLE = 1
    AUTOJUMP_CLOCK = 2




def collapse_exception_group(
    excgroup: BaseExceptionGroup[BaseException],
) -> BaseException:
    pass


@attrs.define(eq=False)
class Deadlines:

    _heap: list[tuple[float, int, CancelScope]] = attrs.Factory(list)
    _active: int = 0

    def add(self, deadline: float, cancel_scope: CancelScope) -> None:
        heappush(self._heap, (deadline, id(cancel_scope), cancel_scope))
        self._active += 1

    def remove(self, deadline: float, cancel_scope: CancelScope) -> None:
        self._active -= 1

    def next_deadline(self) -> float:
        while self._heap:
            deadline, _, cancel_scope = self._heap[0]
            if deadline == cancel_scope._registered_deadline:
                return deadline
            else:
                heappop(self._heap)
        return inf

    def _prune(self) -> None:
        seen = set()
        pruned_heap = []
        for deadline, tiebreaker, cancel_scope in self._heap:
            if deadline == cancel_scope._registered_deadline:
                if cancel_scope in seen:
                    continue
                seen.add(cancel_scope)
                pruned_heap.append((deadline, tiebreaker, cancel_scope))
        assert len(pruned_heap) == self._active
        heapify(pruned_heap)
        self._heap = pruned_heap

    def expire(self, now: float) -> bool:
        did_something = False
        while self._heap and self._heap[0][0] <= now:
            deadline, _, cancel_scope = heappop(self._heap)
            if deadline == cancel_scope._registered_deadline:
                did_something = True
                cancel_scope._cancel(CancelReason(source="deadline"))
        if len(self._heap) > self._active * 2 + DEADLINE_HEAP_MIN_PRUNE_THRESHOLD:
            self._prune()
        return did_something


@attrs.define
class CancelReason:

    source: CancelReasonLiteral
    source_task: str | None = None
    reason: str | None = None


@attrs.define(eq=False)
class CancelStatus:

    _scope: CancelScope = attrs.field(alias="scope")

    effectively_cancelled: bool = False

    _parent: CancelStatus | None = attrs.field(default=None, repr=False, alias="parent")

    _children: set[CancelStatus] = attrs.field(factory=set, init=False, repr=False)

    _tasks: set[Task] = attrs.field(factory=set, init=False, repr=False)

    abandoned_by_misnesting: bool = attrs.field(default=False, init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        if self._parent is not None:
            self._parent._children.add(self)
            self.recalculate()






    def encloses(self, other: CancelStatus | None) -> bool:
        pass

    def close(self) -> None:
        self.parent = None  # now we're not a child of self.parent anymore
        if self._tasks or self._children:
            self._mark_abandoned()

            self.effectively_cancelled = True
            for task in self._tasks:
                task._attempt_delivery_of_any_pending_cancel()
            for child in self._children:
                child.recalculate()


    def recalculate(self) -> None:
        todo = [self]
        while todo:
            current = todo.pop()
            new_state = (
                current._scope.cancel_called
                or current.parent_cancellation_is_visible_to_us
            )
            if new_state != current.effectively_cancelled:
                if (
                    current._scope._cancel_reason is None
                    and current.parent_cancellation_is_visible_to_us
                ):
                    assert current._parent is not None
                    current._scope._cancel_reason = (
                        current._parent._scope._cancel_reason
                    )
                current.effectively_cancelled = new_state
                if new_state:
                    for task in current._tasks:
                        task._attempt_delivery_of_any_pending_cancel()
                todo.extend(current._children)

    def _mark_abandoned(self) -> None:
        self.abandoned_by_misnesting = True
        for child in self._children:
            child._mark_abandoned()

    def effective_deadline(self) -> float:
        if self.effectively_cancelled:
            return -inf
        if self._parent is None or self._scope.shield:
            return self._scope.deadline
        return min(self._scope.deadline, self._parent.effective_deadline())


MISNESTING_ADVICE = """
This is probably a bug in your code, that has caused Trio's internal state to
become corrupted. We'll do our best to recover, but from now on there are
no guarantees.

Typically this is caused by one of the following:
  - yielding within a generator or async generator that's opened a cancel
    scope or nursery (unless the generator is a @contextmanager or
    @asynccontextmanager); see https://github.com/python-trio/trio/issues/638
  - manually calling __enter__ or __exit__ on a trio.CancelScope, or
    __aenter__ or __aexit__ on the object returned by trio.open_nursery();
    doing so correctly is difficult and you should use @[async]contextmanager
    instead, or maybe [Async]ExitStack
  - using [Async]ExitStack to interleave the entries/exits of cancel scopes
    and/or nurseries in a way that couldn't be achieved by some nesting of
    'with' and 'async with' blocks
  - using the low-level coroutine object protocol to execute some parts of
    an async function in a different cancel scope/nursery context than
    other parts
If you don't believe you're doing any of these things, please file a bug:
https://github.com/python-trio/trio/issues/new
"""


@final
@attrs.define(eq=False, repr=False)
class CancelScope:

    _cancel_status: CancelStatus | None = attrs.field(default=None, init=False)
    _has_been_entered: bool = attrs.field(default=False, init=False)
    _registered_deadline: float = attrs.field(default=inf, init=False)
    _cancel_called: bool = attrs.field(default=False, init=False)
    cancelled_caught: bool = attrs.field(default=False, init=False)

    _cancel_reason: CancelReason | None = attrs.field(default=None, init=False)

    _relative_deadline: float = attrs.field(
        default=inf,
        kw_only=True,
        alias="relative_deadline",
    )
    _deadline: float = attrs.field(default=inf, kw_only=True, alias="deadline")
    _shield: bool = attrs.field(default=False, kw_only=True, alias="shield")

    def __attrs_post_init__(self) -> None:
        if isnan(self._deadline):
            raise ValueError("deadline must not be NaN")
        if isnan(self._relative_deadline):
            raise ValueError("relative deadline must not be NaN")
        if self._relative_deadline < 0:
            raise ValueError("timeout must be non-negative")
        if self._relative_deadline != inf and self._deadline != inf:
            raise ValueError(
                "Cannot specify both a deadline and a relative deadline",
            )

    @enable_ki_protection
    def __enter__(self) -> Self:
        task = _core.current_task()
        if self._has_been_entered:
            raise RuntimeError(
                "Each CancelScope may only be used for a single 'with' block",
            )
        self._has_been_entered = True

        if self._relative_deadline != inf:
            assert self._deadline == inf
            self._deadline = current_time() + self._relative_deadline
            self._relative_deadline = inf

        if current_time() >= self._deadline:
            self._cancel(CancelReason(source="deadline"))
        with self._might_change_registered_deadline():
            self._cancel_status = CancelStatus(scope=self, parent=task._cancel_status)
            task._activate_cancel_status(self._cancel_status)
        return self


    @enable_ki_protection
    def __exit__(
        self,
        etype: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:

        remaining_error_after_cancel_scope = self._close(exc)
        if remaining_error_after_cancel_scope is None:
            return True
        elif remaining_error_after_cancel_scope is exc:
            return False
        else:
            old_context = remaining_error_after_cancel_scope.__context__
            try:
                raise remaining_error_after_cancel_scope
            finally:
                _, value, _ = sys.exc_info()
                assert value is remaining_error_after_cancel_scope
                value.__context__ = old_context
                del remaining_error_after_cancel_scope, value, _, exc

    def __repr__(self) -> str:
        if self._cancel_status is not None:
            binding = "active"
        elif self._has_been_entered:
            binding = "exited"
        else:
            binding = "unbound"

        if self._cancel_called:
            state = ", cancelled"
        elif self._deadline == inf:
            state = ""
        else:
            try:
                now = current_time()
            except RuntimeError:  # must be called from async context
                state = ""
            else:
                state = ", deadline is {:.2f} seconds {}".format(
                    abs(self._deadline - now),
                    "from now" if self._deadline >= now else "ago",
                )

        return f"<trio.CancelScope at {id(self):#x}, {binding}{state}>"

    @contextmanager
    @enable_ki_protection
    def _might_change_registered_deadline(self) -> Iterator[None]:
        try:
            yield
        finally:
            old = self._registered_deadline
            if self._cancel_status is None or self._cancel_called:
                new = inf
            else:
                new = self._deadline
            if old != new:
                self._registered_deadline = new
                runner = GLOBAL_RUN_CONTEXT.runner
                if runner.is_guest:
                    old_next_deadline = runner.deadlines.next_deadline()
                if old != inf:
                    runner.deadlines.remove(old, self)
                if new != inf:
                    runner.deadlines.add(new, self)
                if runner.is_guest:
                    new_next_deadline = runner.deadlines.next_deadline()
                    if old_next_deadline != new_next_deadline:
                        runner.force_guest_tick_asap()

    @property
    def deadline(self) -> float:
        pass


    @property
    def relative_deadline(self) -> float:
        pass


    @property
    def is_relative(self) -> bool | None:
        pass

    @property
    def shield(self) -> bool:
        pass


    @enable_ki_protection
    def _cancel(self, cancel_reason: CancelReason | None) -> None:
        """Internal sources of cancellation should use this instead of :meth:`cancel`
        in order to set a more detailed :class:`CancelReason`
        Helper or high-level functions can use `cancel`.
        """
        if self._cancel_called:
            return

        if self._cancel_reason is None:
            self._cancel_reason = cancel_reason

        with self._might_change_registered_deadline():
            self._cancel_called = True

        if self._cancel_status is not None:
            self._cancel_status.recalculate()

    @enable_ki_protection
    def cancel(self, reason: str | None = None) -> None:
        """Cancels this scope immediately.

        The optional ``reason`` argument accepts a string, which will be attached to
        any resulting :exc:`Cancelled` exception to help you understand where that
        cancellation is coming from and why it happened.

        This method is idempotent, i.e., if the scope was already
        cancelled then this method silently does nothing.
        """
        try:
            current_task = repr(_core.current_task())
        except RuntimeError:
            current_task = None
        self._cancel(
            CancelReason(reason=reason, source="explicit", source_task=current_task)
        )

    @property
    def cancel_called(self) -> bool:
        pass




class TaskStatus(Protocol[StatusT_contra]):

    @overload
    def started(self: TaskStatus[None]) -> None: ...

    @overload
    def started(self, value: StatusT_contra) -> None: ...

    def started(self, value: StatusT_contra | None = None) -> None:
        """Tasks call this method to indicate that they have initialized.

        See `nursery.start() <trio.Nursery.start>` for more information.
        """


@attrs.define(eq=False, repr=False, slots=False)
class _TaskStatus(TaskStatus[StatusT]):
    _old_nursery: Nursery
    _new_nursery: Nursery
    _value: StatusT | type[_NoStatus] = _NoStatus

    def __repr__(self) -> str:
        return f"<Task status object at {id(self):#x}>"

    @overload
    def started(self: _TaskStatus[None]) -> None: ...

    @overload
    def started(self: _TaskStatus[StatusT], value: StatusT) -> None: ...

    def started(self, value: StatusT | None = None) -> None:
        if self._value is not _NoStatus:
            raise RuntimeError("called 'started' twice on the same task status")
        self._value = cast("StatusT", value)  # If None, StatusT == None

        assert self._old_nursery._cancel_status is not None
        if self._old_nursery._cancel_status.effectively_cancelled:
            return

        assert not self._new_nursery._closed

        tasks = self._old_nursery._children
        self._old_nursery._children = set()
        for task in tasks:
            task._parent_nursery = self._new_nursery
            task._eventual_parent_nursery = None
            self._new_nursery._children.add(task)

        cancel_status_children = self._old_nursery._cancel_status.children
        cancel_status_tasks = set(self._old_nursery._cancel_status.tasks)
        cancel_status_tasks.discard(self._old_nursery._parent_task)
        for cancel_status in cancel_status_children:
            cancel_status.parent = None
        for task in cancel_status_tasks:
            task._activate_cancel_status(None)
        for cancel_status in cancel_status_children:
            cancel_status.parent = self._new_nursery._cancel_status
        for task in cancel_status_tasks:
            task._activate_cancel_status(self._new_nursery._cancel_status)

        assert not self._old_nursery._children

        self._old_nursery._check_nursery_closed()


@attrs.define(slots=False)
class NurseryManager:

    strict_exception_groups: bool = True

    @enable_ki_protection
    async def __aenter__(self) -> Nursery:
        self._scope = CancelScope()
        self._scope.__enter__()
        self._nursery = Nursery._create(
            current_task(),
            self._scope,
            self.strict_exception_groups,
        )
        return self._nursery

    @enable_ki_protection
    async def __aexit__(
        self,
        etype: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        new_exc = await self._nursery._nested_child_finished(exc)
        combined_error_from_nursery = self._scope._close(new_exc)
        if combined_error_from_nursery is None:
            return True
        elif combined_error_from_nursery is exc:
            return False
        else:
            old_context = combined_error_from_nursery.__context__
            try:
                raise combined_error_from_nursery
            finally:
                _, value, _ = sys.exc_info()
                assert value is combined_error_from_nursery
                value.__context__ = old_context
                del _, combined_error_from_nursery, value, new_exc

    if not TYPE_CHECKING:

        def __enter__(self) -> NoReturn:
            raise RuntimeError(
                "use 'async with open_nursery(...)', not 'with open_nursery(...)'",
            )

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> NoReturn:  # pragma: no cover
            raise AssertionError("Never called, but should be defined")


def open_nursery(
    strict_exception_groups: bool | None = None,
) -> AbstractAsyncContextManager[Nursery]:
    """Returns an async context manager which must be used to create a
    new `Nursery`.

    It does not block on entry; on exit it blocks until all child tasks
    have exited. If no child tasks are running on exit, it will insert a
    schedule point (but no cancellation point) - equivalent to
    :func:`trio.lowlevel.cancel_shielded_checkpoint`. This means a nursery
    is never the source of a cancellation exception, it only propagates it
    from sub-tasks.

    Args:
      strict_exception_groups (bool): Unless set to False, even a single raised exception
          will be wrapped in an exception group. If not specified, uses the value passed
          to :func:`run`, which defaults to true. Setting it to False will be deprecated
          and ultimately removed in a future version of Trio.

    """
    if strict_exception_groups is not None and not strict_exception_groups:
        warn_deprecated(
            "open_nursery(strict_exception_groups=False)",
            version="0.25.0",
            issue=2929,
            instead=(
                "the default value of True and rewrite exception handlers to handle ExceptionGroups. "
                "See https://trio.readthedocs.io/en/stable/reference-core.html#designing-for-multiple-errors"
            ),
            use_triodeprecationwarning=True,
        )

    if strict_exception_groups is None:
        strict_exception_groups = GLOBAL_RUN_CONTEXT.runner.strict_exception_groups

    return NurseryManager(strict_exception_groups=strict_exception_groups)


@final
class Nursery(metaclass=NoPublicConstructor):

    def __init__(
        self,
        parent_task: Task,
        cancel_scope: CancelScope,
        strict_exception_groups: bool,
    ) -> None:
        self._parent_task = parent_task
        self._strict_exception_groups = strict_exception_groups
        parent_task._child_nurseries.append(self)
        self._cancel_status = parent_task._cancel_status
        self.cancel_scope = cancel_scope
        assert self.cancel_scope._cancel_status is self._cancel_status
        self._children: set[Task] = set()
        self._pending_excs: list[BaseException] = []
        self._nested_child_running = True
        self._parent_waiting_in_aexit = False
        self._pending_starts = 0
        self._closed = False

    @property
    def child_tasks(self) -> frozenset[Task]:
        pass

    @property
    def parent_task(self) -> Task:
        pass

    def _add_exc(self, exc: BaseException, reason: CancelReason | None) -> None:
        self._pending_excs.append(exc)
        self.cancel_scope._cancel(reason)

    def _check_nursery_closed(self) -> None:
        if not any([self._nested_child_running, self._children, self._pending_starts]):
            self._closed = True
            if self._parent_waiting_in_aexit:
                self._parent_waiting_in_aexit = False
                GLOBAL_RUN_CONTEXT.runner.reschedule(self._parent_task)

    def _child_finished(
        self,
        task: Task,
        outcome: Outcome[object],
    ) -> None:
        self._children.remove(task)
        if self._closed and not hasattr(self, "_pending_excs"):
            return
        if isinstance(outcome, Error):
            self._add_exc(
                outcome.error,
                CancelReason(
                    source="nursery",
                    source_task=repr(task),
                    reason=f"child task raised exception {outcome.error!r}",
                ),
            )
        self._check_nursery_closed()


    def start_soon(
        self,
        async_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
        *args: Unpack[PosArgT],
        name: object = None,
    ) -> None:
        """Creates a child task, scheduling ``await async_fn(*args)``.

        If you want to run a function and immediately wait for its result,
        then you don't need a nursery; just use ``await async_fn(*args)``.
        If you want to wait for the task to initialize itself before
        continuing, see :meth:`start`, the other fundamental method for
        creating concurrent tasks in Trio.

        Note that this is *not* an async function and you don't use await
        when calling it. It sets up the new task, but then returns
        immediately, *before* the new task has a chance to do anything.
        New tasks may start running in any order, and at any checkpoint the
        scheduler chooses - at latest when the nursery is waiting to exit.

        It's possible to pass a nursery object into another task, which
        allows that task to start new child tasks in the first task's
        nursery.

        The child task inherits its parent nursery's cancel scopes.

        Args:
            async_fn: An async callable.
            args: Positional arguments for ``async_fn``. If you want
                  to pass keyword arguments, use
                  :func:`functools.partial`.
            name: The name for this task. Only used for
                  debugging/introspection
                  (e.g. ``repr(task_obj)``). If this isn't a string,
                  :meth:`start_soon` will try to make it one. A
                  common use case is if you're wrapping a function
                  before spawning a new task, you might pass the
                  original function as the ``name=`` to make
                  debugging easier.

        Raises:
            RuntimeError: If this nursery is no longer open
                          (i.e. its ``async with`` block has
                          exited).
        """
        GLOBAL_RUN_CONTEXT.runner.spawn_impl(async_fn, args, self, name)

    async def start(  # type: ignore[explicit-any]
        self,
        async_fn: Callable[..., Awaitable[object]],
        *args: object,
        name: object = None,
    ) -> Any:
        r"""Creates and initializes a child task.

        Like :meth:`start_soon`, but blocks until the new task has
        finished initializing itself, and optionally returns some
        information from it.

        The ``async_fn`` must accept a ``task_status`` keyword argument,
        and it must make sure that it (or someone) eventually calls
        :meth:`task_status.started() <TaskStatus.started>`.

        The conventional way to define ``async_fn`` is like::

            async def async_fn(arg1, arg2, *, task_status=trio.TASK_STATUS_IGNORED):
                ...  # Caller is blocked waiting for this code to run
                task_status.started()
                ...  # This async code can be interleaved with the caller

        :attr:`trio.TASK_STATUS_IGNORED` is a special global object with
        a do-nothing ``started`` method. This way your function supports
        being called either like ``await nursery.start(async_fn, arg1,
        arg2)`` or directly like ``await async_fn(arg1, arg2)``, and
        either way it can call :meth:`task_status.started() <TaskStatus.started>`
        without worrying about which mode it's in. Defining your function like
        this will make it obvious to readers that it supports being used
        in both modes.

        Before the child calls :meth:`task_status.started() <TaskStatus.started>`,
        it's effectively run underneath the call to :meth:`start`: if it
        raises an exception then that exception is reported by
        :meth:`start`, and does *not* propagate out of the nursery. If
        :meth:`start` is cancelled, then the child task is also
        cancelled.

        When the child calls :meth:`task_status.started() <TaskStatus.started>`,
        it's moved out from underneath :meth:`start` and into the given nursery.

        If the child task passes a value to :meth:`task_status.started(value) <TaskStatus.started>`,
        then :meth:`start` returns this value. Otherwise, it returns ``None``.
        """
        if self._closed:
            raise RuntimeError("Nursery is closed to new arrivals")
        try:
            self._pending_starts += 1
            try:
                async with open_nursery(strict_exception_groups=True) as old_nursery:
                    task_status: _TaskStatus[object | None] = _TaskStatus(
                        old_nursery,
                        self,
                    )
                    thunk = functools.partial(async_fn, task_status=task_status)
                    task = GLOBAL_RUN_CONTEXT.runner.spawn_impl(
                        thunk,
                        args,
                        old_nursery,
                        name,
                    )
                    task._eventual_parent_nursery = self
            except BaseExceptionGroup as exc:
                if len(exc.exceptions) == 1:
                    raise_saving_context(exc.exceptions[0])
                raise TrioInternalError(
                    "Internal nursery should not have multiple tasks. This can be "
                    'caused by the user managing to access the "old" nursery in '
                    "`task_status` and spawning tasks in it.",
                ) from exc

            if task_status._value is _NoStatus:
                raise RuntimeError("child exited without calling task_status.started()")
            return task_status._value
        finally:
            self._pending_starts -= 1
            self._check_nursery_closed()

    def __del__(self) -> None:
        assert not self._children




@final
@attrs.define(eq=False, repr=False)
class Task(metaclass=NoPublicConstructor):  # type: ignore[explicit-any]
    _parent_nursery: Nursery | None
    coro: types.CoroutineType[Any, Outcome[object], Any]  # type: ignore[explicit-any]
    _runner: Runner
    name: str
    context: contextvars.Context
    _counter: int = attrs.field(init=False, factory=itertools.count().__next__)
    _ki_protected: bool

    _next_send_fn: Callable[[Any], object] | None = None  # type: ignore[explicit-any]
    _next_send: Outcome[Any] | BaseException | None = None  # type: ignore[explicit-any]
    _abort_func: Callable[[_core.RaiseCancelT], Abort] | None = None
    custom_sleep_data: Any = None  # type: ignore[explicit-any]

    _child_nurseries: list[Nursery] = attrs.Factory(list)
    _eventual_parent_nursery: Nursery | None = None

    _cancel_points: int = 0
    _schedule_points: int = 0

    def __repr__(self) -> str:
        return f"<Task {self.name!r} at {id(self):#x}>"

    @property
    def parent_nursery(self) -> Nursery | None:
        pass

    @property
    def eventual_parent_nursery(self) -> Nursery | None:
        pass

    @property
    def child_nurseries(self) -> list[Nursery]:
        pass

    def iter_await_frames(self) -> Iterator[tuple[types.FrameType, int]]:
        pass


    _cancel_status: CancelStatus = attrs.field(default=None, repr=False)

    def _activate_cancel_status(self, cancel_status: CancelStatus | None) -> None:
        if self._cancel_status is not None:
            self._cancel_status._tasks.remove(self)
        self._cancel_status = cancel_status  # type: ignore[assignment]
        if self._cancel_status is not None:
            self._cancel_status._tasks.add(self)
            if self._cancel_status.effectively_cancelled:
                self._attempt_delivery_of_any_pending_cancel()

    def _attempt_abort(self, raise_cancel: _core.RaiseCancelT) -> None:

        assert self._abort_func is not None, "FATAL INTERNAL ERROR"

        success = self._abort_func(raise_cancel)
        if type(success) is not Abort:
            raise TrioInternalError("abort function must return Abort enum")
        self._abort_func = None
        if success is Abort.SUCCEEDED:
            self._runner.reschedule(self, capture(raise_cancel))

    def _attempt_delivery_of_any_pending_cancel(self) -> None:
        if self._abort_func is None:
            return
        if not self._cancel_status.effectively_cancelled:
            return

        reason = self._cancel_status._scope._cancel_reason

        def raise_cancel() -> NoReturn:
            if reason is None:
                raise Cancelled._create(source="unknown", reason="misnesting")
            else:
                raise Cancelled._create(
                    source=reason.source,
                    reason=reason.reason,
                    source_task=reason.source_task,
                )

        self._attempt_abort(raise_cancel)

    def _attempt_delivery_of_pending_ki(self) -> None:
        assert self._runner.ki_pending
        if self._abort_func is None:
            return

        def raise_cancel() -> NoReturn:
            self._runner.ki_pending = False
            raise KeyboardInterrupt

        self._attempt_abort(raise_cancel)




@attrs.frozen
class RunStatistics:

    tasks_living: int
    tasks_runnable: int
    seconds_to_next_deadline: float
    io_statistics: IOStatistics
    run_sync_soon_queue_size: int




@attrs.define(eq=False)
class GuestState:  # type: ignore[explicit-any]
    runner: Runner
    run_sync_soon_threadsafe: Callable[[Callable[[], object]], object]
    run_sync_soon_not_threadsafe: Callable[[Callable[[], object]], object]
    done_callback: Callable[[Outcome[Any]], object]  # type: ignore[explicit-any]
    unrolled_run_gen: Generator[float, EventResult, None]
    unrolled_run_next_send: Outcome[Any] = attrs.Factory(lambda: Value(None))  # type: ignore[explicit-any]



@attrs.define(eq=False)
class Runner:  # type: ignore[explicit-any]
    clock: Clock
    instruments: Instruments
    io_manager: TheIOManager
    ki_manager: KIManager
    strict_exception_groups: bool

    _locals: dict[_core.RunVar[Any], object] = attrs.Factory(dict)  # type: ignore[explicit-any]

    runq: deque[Task] = attrs.Factory(deque)
    tasks: set[Task] = attrs.Factory(set)

    deadlines: Deadlines = attrs.Factory(Deadlines)

    init_task: Task | None = None
    system_nursery: Nursery | None = None
    system_context: contextvars.Context = attrs.field(kw_only=True)
    main_task: Task | None = None
    main_task_outcome: Outcome[object] | None = None

    entry_queue: EntryQueue = attrs.Factory(EntryQueue)
    trio_token: TrioToken | None = None
    asyncgens: AsyncGenerators = attrs.Factory(AsyncGenerators)

    clock_autojump_threshold: float = inf

    is_guest: bool = False
    guest_tick_scheduled: bool = False

    def force_guest_tick_asap(self) -> None:
        if self.guest_tick_scheduled:
            return
        self.guest_tick_scheduled = True
        self.io_manager.force_wakeup()

    def close(self) -> None:
        self.io_manager.close()
        self.entry_queue.close()
        self.asyncgens.close()
        if "after_run" in self.instruments:
            self.instruments.call("after_run")
        self.ki_manager.close()

    @_public
    def current_statistics(self) -> RunStatistics:
        """Returns ``RunStatistics``, which contains run-loop-level debugging information.

        Currently, the following fields are defined:

        * ``tasks_living`` (int): The number of tasks that have been spawned
          and not yet exited.
        * ``tasks_runnable`` (int): The number of tasks that are currently
          queued on the run queue (as opposed to blocked waiting for something
          to happen).
        * ``seconds_to_next_deadline`` (float): The time until the next
          pending cancel scope deadline. May be negative if the deadline has
          expired but we haven't yet processed cancellations. May be
          :data:`~math.inf` if there are no pending deadlines.
        * ``run_sync_soon_queue_size`` (int): The number of
          unprocessed callbacks queued via
          :meth:`trio.lowlevel.TrioToken.run_sync_soon`.
        * ``io_statistics`` (object): Some statistics from Trio's I/O
          backend. This always has an attribute ``backend`` which is a string
          naming which operating-system-specific I/O backend is in use; the
          other attributes vary between backends.

        """
        seconds_to_next_deadline = self.deadlines.next_deadline() - self.current_time()
        return RunStatistics(
            tasks_living=len(self.tasks),
            tasks_runnable=len(self.runq),
            seconds_to_next_deadline=seconds_to_next_deadline,
            io_statistics=self.io_manager.statistics(),
            run_sync_soon_queue_size=self.entry_queue.size(),
        )

    @_public
    def current_time(self) -> float:
        """Returns the current time according to Trio's internal clock.

        Returns:
            float: The current time.

        Raises:
            RuntimeError: if not inside a call to :func:`trio.run`.

        """
        return self.clock.current_time()

    @_public
    def current_clock(self) -> Clock:
        pass

    @_public
    def current_root_task(self) -> Task | None:
        pass


    @_public
    def reschedule(
        self, task: Task, next_send: outcome.Outcome[object] = _NO_SEND
    ) -> None:
        """Reschedule the given task with the given
        :class:`outcome.Outcome`.

        See :func:`wait_task_rescheduled` for the gory details.

        There must be exactly one call to :func:`reschedule` for every call to
        :func:`wait_task_rescheduled`. (And when counting, keep in mind that
        returning :data:`Abort.SUCCEEDED` from an abort callback is equivalent
        to calling :func:`reschedule` once.)

        Args:
          task (trio.lowlevel.Task): the task to be rescheduled. Must be blocked
              in a call to :func:`wait_task_rescheduled`.
          next_send (outcome.Outcome): the value (or error) to return (or
              raise) from :func:`wait_task_rescheduled`.

        """
        if next_send is _NO_SEND:
            next_send = Value(None)

        assert task._runner is self
        assert task._next_send_fn is None
        task._next_send_fn = task.coro.send
        task._next_send = next_send
        task._abort_func = None
        task.custom_sleep_data = None
        if not self.runq and self.is_guest:
            self.force_guest_tick_asap()
        self.runq.append(task)
        if "task_scheduled" in self.instruments:
            self.instruments.call("task_scheduled", task)

    def spawn_impl(
        self,
        async_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
        args: tuple[Unpack[PosArgT]],
        nursery: Nursery | None,
        name: object,
        *,
        system_task: bool = False,
        context: contextvars.Context | None = None,
    ) -> Task:

        if nursery is not None and nursery._closed:
            raise RuntimeError("Nursery is closed to new arrivals")
        if nursery is None:
            assert self.init_task is None

        if context is None:
            context = self.system_context.copy() if system_task else copy_context()

        coro = context.run(coroutine_or_error, async_fn, *args)  # type: ignore[arg-type]

        if name is None:
            name = async_fn
        if isinstance(name, functools.partial):
            name = name.func
        if not isinstance(name, str):
            try:
                name = f"{name.__module__}.{name.__qualname__}"  # type: ignore[attr-defined]
            except AttributeError:
                name = repr(name)

        if getattr(coro, "cr_frame", None) is None:
            async def python_wrapper(orig_coro: Awaitable[RetT]) -> RetT:
                return await orig_coro

            coro = python_wrapper(coro)
        assert coro.cr_frame is not None, "Coroutine frame should exist"  # type: ignore[attr-defined]

        task = Task._create(
            coro=coro,
            parent_nursery=nursery,
            runner=self,
            name=name,
            context=context,
            ki_protected=system_task,
        )

        self.tasks.add(task)
        if nursery is not None:
            nursery._children.add(task)
            task._activate_cancel_status(nursery._cancel_status)

        if "task_spawned" in self.instruments:
            self.instruments.call("task_spawned", task)
        self.reschedule(task, None)  # type: ignore[arg-type]
        return task

    def task_exited(self, task: Task, outcome: Outcome[object]) -> None:
        if task._child_nurseries:
            for nursery in task._child_nurseries:
                nursery.cancel_scope._cancel(
                    CancelReason(
                        source="nursery",
                        reason="Parent Task exited prematurely, abandoning this nursery without exiting it properly.",
                        source_task=repr(task),
                    )
                )
                nursery._closed = True

        if task in GLOBAL_PARKING_LOT_BREAKER:
            for lot in GLOBAL_PARKING_LOT_BREAKER[task]:
                lot.break_lot(task)
            del GLOBAL_PARKING_LOT_BREAKER[task]

        if (
            task._cancel_status is not None
            and task._cancel_status.abandoned_by_misnesting
            and task._cancel_status.parent is None
        ) or task._child_nurseries:
            reason = "Nursery" if task._child_nurseries else "Cancel scope"
            try:
                raise RuntimeError(
                    f"{reason} stack corrupted: {reason} surrounding "
                    f"{task!r} was closed before the task exited\n{MISNESTING_ADVICE}",
                )
            except RuntimeError as new_exc:
                if isinstance(outcome, Error):
                    new_exc.__context__ = outcome.error
                outcome = Error(new_exc)

        task._activate_cancel_status(None)
        self.tasks.remove(task)
        if task is self.init_task:
            outcome.unwrap()
            if self.tasks:  # pragma: no cover
                raise TrioInternalError
        else:
            if task is self.main_task:
                self.main_task_outcome = outcome
                outcome = Value(None)
            assert task._parent_nursery is not None, task
            task._parent_nursery._child_finished(task, outcome)

        if "task_exited" in self.instruments:
            self.instruments.call("task_exited", task)


    @_public
    def spawn_system_task(
        self,
        async_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
        *args: Unpack[PosArgT],
        name: object = None,
        context: contextvars.Context | None = None,
    ) -> Task:
        """Spawn a "system" task.

        System tasks have a few differences from regular tasks:

        * They don't need an explicit nursery; instead they go into the
          internal "system nursery".

        * If a system task raises an exception, then it's converted into a
          :exc:`~trio.TrioInternalError` and *all* tasks are cancelled. If you
          write a system task, you should be careful to make sure it doesn't
          crash.

        * System tasks are automatically cancelled when the main task exits.

        * By default, system tasks have :exc:`KeyboardInterrupt` protection
          *enabled*. If you want your task to be interruptible by control-C,
          then you need to use :func:`disable_ki_protection` explicitly (and
          come up with some plan for what to do with a
          :exc:`KeyboardInterrupt`, given that system tasks aren't allowed to
          raise exceptions).

        * System tasks do not inherit context variables from their creator.

        Towards the end of a call to :meth:`trio.run`, after the main
        task and all system tasks have exited, the system nursery
        becomes closed. At this point, new calls to
        :func:`spawn_system_task` will raise ``RuntimeError("Nursery
        is closed to new arrivals")`` instead of creating a system
        task. It's possible to encounter this state either in
        a ``finally`` block in an async generator, or in a callback
        passed to :meth:`TrioToken.run_sync_soon` at the right moment.

        Args:
          async_fn: An async callable.
          args: Positional arguments for ``async_fn``. If you want to pass
              keyword arguments, use :func:`functools.partial`.
          name: The name for this task. Only used for debugging/introspection
              (e.g. ``repr(task_obj)``). If this isn't a string,
              :func:`spawn_system_task` will try to make it one. A common use
              case is if you're wrapping a function before spawning a new
              task, you might pass the original function as the ``name=`` to
              make debugging easier.
          context: An optional ``contextvars.Context`` object with context variables
              to use for this task. You would normally get a copy of the current
              context with ``context = contextvars.copy_context()`` and then you would
              pass that ``context`` object here.

        Returns:
          Task: the newly spawned task

        """
        return self.spawn_impl(
            async_fn,
            args,
            self.system_nursery,
            name,
            system_task=True,
            context=context,
        )



    @_public
    def current_trio_token(self) -> TrioToken:
        """Retrieve the :class:`TrioToken` for the current call to
        :func:`trio.run`.

        """
        if self.trio_token is None:
            self.trio_token = TrioToken._create(self.entry_queue)
        return self.trio_token


    ki_pending: bool = False





    waiting_for_idle: Any = attrs.Factory(SortedDict)  # type: ignore[explicit-any]

    @_public
    async def wait_all_tasks_blocked(self, cushion: float = 0.0) -> None:
        """Block until there are no runnable tasks.

        This is useful in testing code when you want to give other tasks a
        chance to "settle down". The calling task is blocked, and doesn't wake
        up until all other tasks are also blocked for at least ``cushion``
        seconds. (Setting a non-zero ``cushion`` is intended to handle cases
        like two tasks talking to each other over a local socket, where we
        want to ignore the potential brief moment between a send and receive
        when all tasks are blocked.)

        Note that ``cushion`` is measured in *real* time, not the Trio clock
        time.

        If there are multiple tasks blocked in :func:`wait_all_tasks_blocked`,
        then the one with the shortest ``cushion`` is the one woken (and
        this task becoming unblocked resets the timers for the remaining
        tasks). If there are multiple tasks that have exactly the same
        ``cushion``, then all are woken.

        You should also consider :class:`trio.testing.Sequencer`, which
        provides a more explicit way to control execution ordering within a
        test, and will often produce more readable tests.

        Example:
          Here's an example of one way to test that Trio's locks are fair: we
          take the lock in the parent, start a child, wait for the child to be
          blocked waiting for the lock (!), and then check that we can't
          release and immediately re-acquire the lock::

             async def lock_taker(lock):
                 await lock.acquire()
                 lock.release()

             async def test_lock_fairness():
                 lock = trio.Lock()
                 await lock.acquire()
                 async with trio.open_nursery() as nursery:
                     nursery.start_soon(lock_taker, lock)
                     # child hasn't run yet, we have the lock
                     assert lock.locked()
                     assert lock._owner is trio.lowlevel.current_task()
                     await trio.testing.wait_all_tasks_blocked()
                     # now the child has run and is blocked on lock.acquire(), we
                     # still have the lock
                     assert lock.locked()
                     assert lock._owner is trio.lowlevel.current_task()
                     lock.release()
                     try:
                         # The child has a prior claim, so we can't have it
                         lock.acquire_nowait()
                     except trio.WouldBlock:
                         assert lock._owner is not trio.lowlevel.current_task()
                         print("PASS")
                     else:
                         print("FAIL")

        """
        task = current_task()
        key = (cushion, id(task))
        self.waiting_for_idle[key] = task


        await wait_task_rescheduled(abort)




def setup_runner(
    clock: Clock | None,
    instruments: Sequence[Instrument],
    restrict_keyboard_interrupt_to_checkpoints: bool,
    strict_exception_groups: bool,
) -> Runner:
    """Create a Runner object and install it as the GLOBAL_RUN_CONTEXT."""
    if in_trio_run():
        raise RuntimeError("Attempted to call run() from inside a run()")

    if clock is None:
        clock = SystemClock()
    instrument_group = Instruments(instruments)
    io_manager = TheIOManager()
    system_context = copy_context()
    ki_manager = KIManager()

    runner = Runner(
        clock=clock,
        instruments=instrument_group,
        io_manager=io_manager,
        system_context=system_context,
        ki_manager=ki_manager,
        strict_exception_groups=strict_exception_groups,
    )
    runner.asyncgens.install_hooks(runner)

    ki_manager.install(runner.deliver_ki, restrict_keyboard_interrupt_to_checkpoints)

    GLOBAL_RUN_CONTEXT.runner = runner
    return runner


def run(
    async_fn: Callable[[Unpack[PosArgT]], Awaitable[RetT]],
    *args: Unpack[PosArgT],
    clock: Clock | None = None,
    instruments: Sequence[Instrument] = (),
    restrict_keyboard_interrupt_to_checkpoints: bool = False,
    strict_exception_groups: bool = True,
) -> RetT:
    """Run a Trio-flavored async function, and return the result.

    Calling::

       run(async_fn, *args)

    is the equivalent of::

       await async_fn(*args)

    except that :func:`run` can (and must) be called from a synchronous
    context.

    This is Trio's main entry point. Almost every other function in Trio
    requires that you be inside a call to :func:`run`.

    Args:
      async_fn: An async function.

      args: Positional arguments to be passed to *async_fn*. If you need to
          pass keyword arguments, then use :func:`functools.partial`.

      clock: ``None`` to use the default system-specific monotonic clock;
          otherwise, an object implementing the :class:`trio.abc.Clock`
          interface, like (for example) a :class:`trio.testing.MockClock`
          instance.

      instruments (list of :class:`trio.abc.Instrument` objects): Any
          instrumentation you want to apply to this run. This can also be
          modified during the run; see :ref:`instrumentation`.

      restrict_keyboard_interrupt_to_checkpoints (bool): What happens if the
          user hits control-C while :func:`run` is running? If this argument
          is False (the default), then you get the standard Python behavior: a
          :exc:`KeyboardInterrupt` exception will immediately interrupt
          whatever task is running (or if no task is running, then Trio will
          wake up a task to be interrupted). Alternatively, if you set this
          argument to True, then :exc:`KeyboardInterrupt` delivery will be
          delayed: it will be *only* be raised at :ref:`checkpoints
          <checkpoints>`, like a :exc:`Cancelled` exception.

          The default behavior is nice because it means that even if you
          accidentally write an infinite loop that never executes any
          checkpoints, then you can still break out of it using control-C.
          The alternative behavior is nice if you're paranoid about a
          :exc:`KeyboardInterrupt` at just the wrong place leaving your
          program in an inconsistent state, because it means that you only
          have to worry about :exc:`KeyboardInterrupt` at the exact same
          places where you already have to worry about :exc:`Cancelled`.

          This setting has no effect if your program has registered a custom
          SIGINT handler, or if :func:`run` is called from anywhere but the
          main thread (this is a Python limitation), or if you use
          :func:`open_signal_receiver` to catch SIGINT.

      strict_exception_groups (bool): Unless set to False, nurseries will always wrap
          even a single raised exception in an exception group. This can be overridden
          on the level of individual nurseries. Setting it to False will be deprecated
          and ultimately removed in a future version of Trio.

    Returns:
      Whatever ``async_fn`` returns.

    Raises:
      TrioInternalError: if an unexpected error is encountered inside Trio's
          internal machinery. This is a bug and you should `let us know
          <https://github.com/python-trio/trio/issues>`__.

      Anything else: if ``async_fn`` raises an exception, then :func:`run`
          propagates it.

    """
    if strict_exception_groups is not None and not strict_exception_groups:
        warn_deprecated(
            "trio.run(..., strict_exception_groups=False)",
            version="0.25.0",
            issue=2929,
            instead=(
                "the default value of True and rewrite exception handlers to handle ExceptionGroups. "
                "See https://trio.readthedocs.io/en/stable/reference-core.html#designing-for-multiple-errors"
            ),
            use_triodeprecationwarning=True,
        )

    __tracebackhide__ = True

    runner = setup_runner(
        clock,
        instruments,
        restrict_keyboard_interrupt_to_checkpoints,
        strict_exception_groups,
    )

    prev_library, sniffio_library.name = sniffio_library.name, "trio"
    try:
        gen = unrolled_run(runner, async_fn, args)
        next_send: EventResult = None  # type: ignore[assignment]
        while True:
            try:
                timeout = gen.send(next_send)
            except StopIteration:
                break
            next_send = runner.io_manager.get_events(timeout)
    finally:
        sniffio_library.name = prev_library
    if isinstance(runner.main_task_outcome, Value):
        return cast("RetT", runner.main_task_outcome.value)
    elif isinstance(runner.main_task_outcome, Error):
        raise runner.main_task_outcome.error
    else:  # pragma: no cover
        raise AssertionError(runner.main_task_outcome)


def start_guest_run(  # type: ignore[explicit-any]
    async_fn: Callable[..., Awaitable[RetT]],
    *args: object,
    run_sync_soon_threadsafe: Callable[[Callable[[], object]], object],
    done_callback: Callable[[outcome.Outcome[RetT]], object],
    run_sync_soon_not_threadsafe: (
        Callable[[Callable[[], object]], object] | None
    ) = None,
    host_uses_signal_set_wakeup_fd: bool = False,
    clock: Clock | None = None,
    instruments: Sequence[Instrument] = (),
    restrict_keyboard_interrupt_to_checkpoints: bool = False,
    strict_exception_groups: bool = True,
) -> None:
    pass


_MAX_TIMEOUT: Final = 24 * 60 * 60


@enable_ki_protection
def unrolled_run(
    runner: Runner,
    async_fn: Callable[[Unpack[PosArgT]], Awaitable[object]],
    args: tuple[Unpack[PosArgT]],
    host_uses_signal_set_wakeup_fd: bool = False,
) -> Generator[float, EventResult, None]:
    __tracebackhide__ = True

    try:
        if not host_uses_signal_set_wakeup_fd:
            runner.entry_queue.wakeup.wakeup_on_signals()

        if "before_run" in runner.instruments:
            runner.instruments.call("before_run")
        runner.clock.start_clock()
        runner.init_task = runner.spawn_impl(
            runner.init,
            (async_fn, args),
            None,
            "<init>",
            system_task=True,
        )

        while runner.tasks:
            if runner.runq:
                timeout: float = 0
            else:
                deadline = runner.deadlines.next_deadline()
                timeout = runner.clock.deadline_to_sleep_time(deadline)
            timeout = min(max(0, timeout), _MAX_TIMEOUT)

            idle_primed = None
            if runner.waiting_for_idle:
                cushion, _ = runner.waiting_for_idle.keys()[0]
                if cushion < timeout:
                    timeout = cushion
                    idle_primed = IdlePrimedTypes.WAITING_FOR_IDLE
            elif runner.clock_autojump_threshold < timeout:
                timeout = runner.clock_autojump_threshold
                idle_primed = IdlePrimedTypes.AUTOJUMP_CLOCK

            if "before_io_wait" in runner.instruments:
                runner.instruments.call("before_io_wait", timeout)

            events = yield timeout
            runner.io_manager.process_events(events)

            if "after_io_wait" in runner.instruments:
                runner.instruments.call("after_io_wait", timeout)

            now = runner.clock.current_time()
            if runner.deadlines.expire(now):
                idle_primed = None

            if idle_primed is not None and not runner.runq and not events:
                if idle_primed is IdlePrimedTypes.WAITING_FOR_IDLE:
                    while runner.waiting_for_idle:
                        key, task = runner.waiting_for_idle.peekitem(0)
                        if key[0] == cushion:
                            del runner.waiting_for_idle[key]
                            runner.reschedule(task)
                        else:
                            break
                else:
                    assert idle_primed is IdlePrimedTypes.AUTOJUMP_CLOCK
                    assert isinstance(runner.clock, _core.MockClock)
                    runner.clock._autojump()

            batch = list(runner.runq)
            runner.runq.clear()
            if _ALLOW_DETERMINISTIC_SCHEDULING:
                batch.sort(key=lambda t: t._counter)
                _r.shuffle(batch)
            else:
                if _r.random() < 0.5:
                    batch.reverse()
            while batch:
                task = batch.pop()
                GLOBAL_RUN_CONTEXT.task = task

                if "before_task_step" in runner.instruments:
                    runner.instruments.call("before_task_step", task)

                next_send_fn = task._next_send_fn
                next_send = task._next_send
                task._next_send_fn = task._next_send = None
                final_outcome: Outcome[object] | None = None

                assert next_send_fn is not None

                try:
                    msg = task.context.run(next_send_fn, next_send)
                except StopIteration as stop_iteration:
                    final_outcome = Value(stop_iteration.value)
                except BaseException as task_exc:
                    tb = task_exc.__traceback__
                    for _ in range(1 + CONTEXT_RUN_TB_FRAMES):
                        if tb is not None:  # pragma: no branch
                            tb = tb.tb_next
                    final_outcome = Error(task_exc.with_traceback(tb))
                    del tb

                if final_outcome is not None:
                    runner.task_exited(task, final_outcome)
                    final_outcome = None
                else:
                    task._schedule_points += 1
                    if msg is CancelShieldedCheckpoint:
                        runner.reschedule(task)
                    elif type(msg) is WaitTaskRescheduled:
                        task._cancel_points += 1
                        task._abort_func = msg.abort_func
                        if runner.ki_pending and task is runner.main_task:
                            task._attempt_delivery_of_pending_ki()
                        task._attempt_delivery_of_any_pending_cancel()
                    elif type(msg) is PermanentlyDetachCoroutineObject:
                        runner.task_exited(task, msg.final_outcome)
                    else:
                        exc = TypeError(
                            f"trio.run received unrecognized yield message {msg!r}. "
                            "Are you trying to use a library written for some "
                            "other framework like asyncio? That won't work "
                            "without some kind of compatibility shim.",
                        )
                        runner.reschedule(task, exc)  # type: ignore[arg-type]
                        task._next_send_fn = task.coro.throw
                    del msg

                if "after_task_step" in runner.instruments:
                    runner.instruments.call("after_task_step", task)
                del GLOBAL_RUN_CONTEXT.task
                del task, next_send, next_send_fn

    except GeneratorExit:
        warnings.warn(
            RuntimeWarning(
                "Trio guest run got abandoned without properly finishing... "
                "weird stuff might happen",
            ),
            stacklevel=1,
        )
    except TrioInternalError:
        raise
    except BaseException as exc:
        raise TrioInternalError("internal error in Trio - please file a bug!") from exc
    finally:
        runner.close()
        GLOBAL_RUN_CONTEXT.__dict__.clear()

        if runner.ki_pending:
            ki = KeyboardInterrupt()
            if isinstance(runner.main_task_outcome, Error):
                ki.__context__ = runner.main_task_outcome.error
            runner.main_task_outcome = Error(ki)




class _TaskStatusIgnored(TaskStatus[object]):
    def __repr__(self) -> str:
        return "TASK_STATUS_IGNORED"

    def started(self, value: object = None) -> None:
        pass


TASK_STATUS_IGNORED: Final[TaskStatus[object]] = _TaskStatusIgnored()


def current_task() -> Task:
    """Return the :class:`Task` object representing the current task.

    Returns:
      Task: the :class:`Task` that called :func:`current_task`.

    """

    try:
        return GLOBAL_RUN_CONTEXT.task
    except AttributeError:
        raise RuntimeError("must be called from async context") from None


def current_effective_deadline() -> float:
    """Returns the current effective deadline for the current task.

    This function examines all the cancellation scopes that are currently in
    effect (taking into account shielding), and returns the deadline that will
    expire first.

    One example of where this might be is useful is if your code is trying to
    decide whether to begin an expensive operation like an RPC call, but wants
    to skip it if it knows that it can't possibly complete in the available
    time. Another example would be if you're using a protocol like gRPC that
    `propagates timeout information to the remote peer
    <http://www.grpc.io/docs/guides/concepts.html#deadlines>`__; this function
    gives a way to fetch that information so you can send it along.

    If this is called in a context where a cancellation is currently active
    (i.e., a blocking call will immediately raise :exc:`Cancelled`), then
    returned deadline is ``-inf``. If it is called in a context where no
    scopes have a deadline set, it returns ``inf``.

    Returns:
        float: the effective deadline, as an absolute time.

    """
    return current_task()._cancel_status.effective_deadline()


async def checkpoint() -> None:
    """A pure :ref:`checkpoint <checkpoints>`.

    This checks for cancellation and allows other tasks to be scheduled,
    without otherwise blocking.

    Note that the scheduler has the option of ignoring this and continuing to
    run the current task if it decides this is appropriate (e.g. for increased
    efficiency).

    Equivalent to ``await trio.sleep(0)`` (which is implemented by calling
    :func:`checkpoint`.)

    """
    await cancel_shielded_checkpoint()
    task = current_task()
    task._cancel_points += 1
    if task._cancel_status.effectively_cancelled or (
        task is task._runner.main_task and task._runner.ki_pending
    ):
        cs = CancelScope(deadline=-inf)
        if (
            task._cancel_status._scope._cancel_reason is None
            and task is task._runner.main_task
            and task._runner.ki_pending
        ):
            task._cancel_status._scope._cancel_reason = CancelReason(
                source="KeyboardInterrupt"
            )
        assert task._cancel_status._scope._cancel_reason is not None
        cs._cancel_reason = task._cancel_status._scope._cancel_reason
        with cs:
            await _core.wait_task_rescheduled(lambda _: _core.Abort.SUCCEEDED)


async def checkpoint_if_cancelled() -> None:
    """Issue a :ref:`checkpoint <checkpoints>` if the calling context has been
    cancelled.

    Equivalent to (but potentially more efficient than)::

        if trio.current_effective_deadline() == -inf:
            await trio.lowlevel.checkpoint()

    This is either a no-op, or else it allow other tasks to be scheduled and
    then raises :exc:`trio.Cancelled`.

    Typically used together with :func:`cancel_shielded_checkpoint`.

    """
    task = current_task()
    if task._cancel_status.effectively_cancelled or (
        task is task._runner.main_task and task._runner.ki_pending
    ):
        await _core.checkpoint()
        raise AssertionError("this should never happen")  # pragma: no cover
    task._cancel_points += 1


def in_trio_run() -> bool:
    """Check whether we are in a Trio run.
    This returns `True` if and only if :func:`~trio.current_time` will succeed.

    See also the discussion of differing ways of :ref:`detecting Trio <trio_contexts>`.
    """
    return hasattr(GLOBAL_RUN_CONTEXT, "runner")


def in_trio_task() -> bool:
    """Check whether we are in a Trio task.
    This returns `True` if and only if :func:`~trio.lowlevel.current_task` will succeed.

    See also the discussion of differing ways of :ref:`detecting Trio <trio_contexts>`.
    """
    return hasattr(GLOBAL_RUN_CONTEXT, "task")


if "sphinx.ext.autodoc" in sys.modules:
    from ._generated_io_epoll import *
    from ._generated_io_kqueue import *
    from ._generated_io_windows import *

if sys.platform == "win32":
    from ._generated_io_windows import *
    from ._io_windows import (
        EventResult as EventResult,
        WindowsIOManager as TheIOManager,
        _WindowsStatistics as IOStatistics,
    )
elif (
    sys.platform == "linux"
    or sys.platform == "android"
    or (not TYPE_CHECKING and hasattr(select, "epoll"))
):
    from ._generated_io_epoll import *
    from ._io_epoll import (
        EpollIOManager as TheIOManager,
        EventResult as EventResult,
        _EpollStatistics as IOStatistics,
    )
elif TYPE_CHECKING or hasattr(select, "kqueue"):
    from ._generated_io_kqueue import *
    from ._io_kqueue import (
        EventResult as EventResult,
        KqueueIOManager as TheIOManager,
        _KqueueStatistics as IOStatistics,
    )
else:  # pragma: no cover
    _patchers = sorted({"eventlet", "gevent"}.intersection(sys.modules))
    if _patchers:
        raise NotImplementedError(
            "unsupported platform or primitives Trio depends on are monkey-patched out by "
            + ", ".join(_patchers),
        )

    raise NotImplementedError("unsupported platform")

from ._generated_instrumentation import *
from ._generated_run import *
