from __future__ import annotations

import contextlib
import contextvars
import inspect
import queue as stdlib_queue
import threading
from itertools import count
from typing import TYPE_CHECKING, Generic, TypeVar

import attrs
import outcome
from attrs import define
from sniffio import current_async_library_cvar

import trio

from ._core import (
    RunVar,
    TrioToken,
    checkpoint,
    disable_ki_protection,
    enable_ki_protection,
    start_thread_soon,
)
from ._sync import CapacityLimiter, Event
from ._util import coroutine_or_error

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator

    from typing_extensions import TypeVarTuple, Unpack

    from trio._core._traps import RaiseCancelT

    Ts = TypeVarTuple("Ts")

RetT = TypeVar("RetT")


class _ParentTaskData(threading.local):

    token: TrioToken
    abandon_on_cancel: bool
    cancel_register: list[RaiseCancelT | None]
    task_register: list[trio.lowlevel.Task | None]


PARENT_TASK_DATA = _ParentTaskData()

_limiter_local: RunVar[CapacityLimiter] = RunVar("limiter")
DEFAULT_LIMIT = 40
_thread_counter = count()


@define
class _ActiveThreadCount:
    count: int
    event: Event


_active_threads_local: RunVar[_ActiveThreadCount] = RunVar("active_threads")


@contextlib.contextmanager
def _track_active_thread() -> Generator[None, None, None]:
    try:
        active_threads_local = _active_threads_local.get()
    except LookupError:
        active_threads_local = _ActiveThreadCount(0, Event())
        _active_threads_local.set(active_threads_local)

    active_threads_local.count += 1
    try:
        yield
    finally:
        active_threads_local.count -= 1
        if active_threads_local.count == 0:
            active_threads_local.event.set()
            active_threads_local.event = Event()


async def wait_all_threads_completed() -> None:
    pass


def active_thread_count() -> int:
    pass


def current_default_thread_limiter() -> CapacityLimiter:
    """Get the default `~trio.CapacityLimiter` used by
    `trio.to_thread.run_sync`.

    The most common reason to call this would be if you want to modify its
    :attr:`~trio.CapacityLimiter.total_tokens` attribute.

    """
    try:
        limiter = _limiter_local.get()
    except LookupError:
        limiter = CapacityLimiter(DEFAULT_LIMIT)
        _limiter_local.set(limiter)
    return limiter


@attrs.frozen(eq=False, slots=False)
class ThreadPlaceholder:
    name: str


@attrs.frozen(eq=False, slots=False)
class Run(Generic[RetT]):  # type: ignore[explicit-any]
    afn: Callable[..., Awaitable[RetT]]  # type: ignore[explicit-any]
    args: tuple[object, ...]
    context: contextvars.Context = attrs.field(
        init=False,
        factory=contextvars.copy_context,
    )
    queue: stdlib_queue.SimpleQueue[outcome.Outcome[RetT]] = attrs.field(
        init=False,
        factory=stdlib_queue.SimpleQueue,
    )


    async def run(self) -> None:
        task = trio.lowlevel.current_task()
        old_context = task.context
        task.context = self.context.copy()
        await trio.lowlevel.cancel_shielded_checkpoint()
        result = await outcome.acapture(self.unprotected_afn)
        task.context = old_context
        await trio.lowlevel.cancel_shielded_checkpoint()
        self.queue.put_nowait(result)





@attrs.frozen(eq=False, slots=False)
class RunSync(Generic[RetT]):  # type: ignore[explicit-any]
    fn: Callable[..., RetT]  # type: ignore[explicit-any]
    args: tuple[object, ...]
    context: contextvars.Context = attrs.field(
        init=False,
        factory=contextvars.copy_context,
    )
    queue: stdlib_queue.SimpleQueue[outcome.Outcome[RetT]] = attrs.field(
        init=False,
        factory=stdlib_queue.SimpleQueue,
    )


    def run_sync(self) -> None:
        result = outcome.capture(self.unprotected_fn)
        self.queue.put_nowait(result)




@enable_ki_protection
async def to_thread_run_sync(
    sync_fn: Callable[[Unpack[Ts]], RetT],
    *args: Unpack[Ts],
    thread_name: str | None = None,
    abandon_on_cancel: bool = False,
    limiter: CapacityLimiter | None = None,
) -> RetT:
    """Convert a blocking operation into an async operation using a thread.

    These two lines are equivalent::

        sync_fn(*args)
        await trio.to_thread.run_sync(sync_fn, *args)

    except that if ``sync_fn`` takes a long time, then the first line will
    block the Trio loop while it runs, while the second line allows other Trio
    tasks to continue working while ``sync_fn`` runs. This is accomplished by
    pushing the call to ``sync_fn(*args)`` off into a worker thread.

    From inside the worker thread, you can get back into Trio using the
    functions in `trio.from_thread`.

    Args:
      sync_fn: An arbitrary synchronous callable.
      *args: Positional arguments to pass to sync_fn. If you need keyword
          arguments, use :func:`functools.partial`.
      abandon_on_cancel (bool): Whether to abandon this thread upon
          cancellation of this operation. See discussion below.
      thread_name (str): Optional string to set the name of the thread.
          Will always set `threading.Thread.name`, but only set the os name
          if pthread.h is available (i.e. most POSIX installations).
          pthread names are limited to 15 characters, and can be read from
          ``/proc/<PID>/task/<SPID>/comm`` or with ``ps -eT``, among others.
          Defaults to ``{sync_fn.__name__|None} from {trio.lowlevel.current_task().name}``.
      limiter (None, or CapacityLimiter-like object):
          An object used to limit the number of simultaneous threads. Most
          commonly this will be a `~trio.CapacityLimiter`, but it could be
          anything providing compatible
          :meth:`~trio.CapacityLimiter.acquire_on_behalf_of` and
          :meth:`~trio.CapacityLimiter.release_on_behalf_of` methods. This
          function will call ``acquire_on_behalf_of`` before starting the
          thread, and ``release_on_behalf_of`` after the thread has finished.

          If None (the default), uses the default `~trio.CapacityLimiter`, as
          returned by :func:`current_default_thread_limiter`.

    **Cancellation handling**: Cancellation is a tricky issue here, because
    neither Python nor the operating systems it runs on provide any general
    mechanism for cancelling an arbitrary synchronous function running in a
    thread. This function will always check for cancellation on entry, before
    starting the thread. But once the thread is running, there are two ways it
    can handle being cancelled:

    * If ``abandon_on_cancel=False``, the function ignores the cancellation and
      keeps going, just like if we had called ``sync_fn`` synchronously. This
      is the default behavior.

    * If ``abandon_on_cancel=True``, then this function immediately raises
      `~trio.Cancelled`. In this case **the thread keeps running in
      background** – we just abandon it to do whatever it's going to do, and
      silently discard any return value or errors that it raises. Only use
      this if you know that the operation is safe and side-effect free. (For
      example: :func:`trio.socket.getaddrinfo` uses a thread with
      ``abandon_on_cancel=True``, because it doesn't really affect anything if a
      stray hostname lookup keeps running in the background.)

      The ``limiter`` is only released after the thread has *actually*
      finished – which in the case of cancellation may be some time after this
      function has returned. If :func:`trio.run` finishes before the thread
      does, then the limiter release method will never be called at all.

    .. warning::

       You should not use this function to call long-running CPU-bound
       functions! In addition to the usual GIL-related reasons why using
       threads for CPU-bound work is not very effective in Python, there is an
       additional problem: on CPython, `CPU-bound threads tend to "starve out"
       IO-bound threads <https://bugs.python.org/issue7946>`__, so using
       threads for CPU-bound work is likely to adversely affect the main
       thread running Trio. If you need to do this, you're better off using a
       worker process, or perhaps PyPy (which still has a GIL, but may do a
       better job of fairly allocating CPU time between threads).

    Returns:
      Whatever ``sync_fn(*args)`` returns.

    Raises:
      Exception: Whatever ``sync_fn(*args)`` raises.

    """
    await trio.lowlevel.checkpoint_if_cancelled()
    abandon_bool = bool(abandon_on_cancel)
    if limiter is None:
        limiter = current_default_thread_limiter()

    task_register: list[trio.lowlevel.Task | None] = [trio.lowlevel.current_task()]
    cancel_register: list[RaiseCancelT | None] = [None]  # type: ignore[assignment]
    name = f"trio.to_thread.run_sync-{next(_thread_counter)}"
    placeholder = ThreadPlaceholder(name)


    current_trio_token = trio.lowlevel.current_trio_token()

    if thread_name is None:
        thread_name = f"{getattr(sync_fn, '__name__', None)} from {trio.lowlevel.current_task().name}"


    context = contextvars.copy_context()
    context.run(current_async_library_cvar.set, None)


    await limiter.acquire_on_behalf_of(placeholder)
    with _track_active_thread():
        try:
            start_thread_soon(worker_fn, deliver_worker_fn_result, thread_name)
        except:
            limiter.release_on_behalf_of(placeholder)
            raise


        while True:
            msg_from_thread: outcome.Outcome[RetT] | Run[object] | RunSync[object] = (
                await trio.lowlevel.wait_task_rescheduled(abort)
            )
            if isinstance(msg_from_thread, outcome.Outcome):
                return msg_from_thread.unwrap()
            elif isinstance(msg_from_thread, Run):
                await msg_from_thread.run()
            elif isinstance(msg_from_thread, RunSync):
                msg_from_thread.run_sync()
            else:  # pragma: no cover, internal debugging guard TODO: use assert_never
                raise TypeError(
                    f"trio.to_thread.run_sync received unrecognized thread message {msg_from_thread!r}.",
                )
            del msg_from_thread


def from_thread_check_cancelled() -> None:
    """Raise `trio.Cancelled` if the associated Trio task entered a cancelled status.

     Only applicable to threads spawned by `trio.to_thread.run_sync`. Poll to allow
     ``abandon_on_cancel=False`` threads to raise :exc:`~trio.Cancelled` at a suitable
     place, or to end abandoned ``abandon_on_cancel=True`` threads sooner than they may
     otherwise.

    Raises:
        Cancelled: If the corresponding call to `trio.to_thread.run_sync` has had a
            delivery of cancellation attempted against it, regardless of the value of
            ``abandon_on_cancel`` supplied as an argument to it.
        RuntimeError: If this thread is not spawned from `trio.to_thread.run_sync`.

    .. note::

       To be precise, :func:`~trio.from_thread.check_cancelled` checks whether the task
       running :func:`trio.to_thread.run_sync` has ever been cancelled since the last
       time it was running a :func:`trio.from_thread.run` or :func:`trio.from_thread.run_sync`
       function. It may raise `trio.Cancelled` even if a cancellation occurred that was
       later hidden by a modification to `trio.CancelScope.shield` between the cancelled
       `~trio.CancelScope` and :func:`trio.to_thread.run_sync`. This differs from the
       behavior of normal Trio checkpoints, which raise `~trio.Cancelled` only if the
       cancellation is still active when the checkpoint executes. The distinction here is
       *exceedingly* unlikely to be relevant to your application, but we mention it
       for completeness.
    """
    try:
        raise_cancel = PARENT_TASK_DATA.cancel_register[0]
    except AttributeError:
        raise RuntimeError(
            "this thread wasn't created by Trio, can't check for cancellation",
        ) from None
    if raise_cancel is not None:
        raise_cancel()


def _send_message_to_trio(
    trio_token: TrioToken | None,
    message_to_trio: Run[RetT] | RunSync[RetT],
) -> RetT:
    pass


def from_thread_run(
    afn: Callable[[Unpack[Ts]], Awaitable[RetT]],
    *args: Unpack[Ts],
    trio_token: TrioToken | None = None,
) -> RetT:
    pass


def from_thread_run_sync(
    fn: Callable[[Unpack[Ts]], RetT],
    *args: Unpack[Ts],
    trio_token: TrioToken | None = None,
) -> RetT:
    pass
