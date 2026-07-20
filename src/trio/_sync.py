from __future__ import annotations

import math
from typing import TYPE_CHECKING, Literal, Protocol, TypeVar

import attrs

import trio

from . import _core
from ._core import (
    Abort,
    ParkingLot,
    RaiseCancelT,
    add_parking_lot_breaker,
    enable_ki_protection,
    remove_parking_lot_breaker,
)
from ._deprecate import warn_deprecated
from ._util import final

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from typing_extensions import deprecated

    from ._core import Task
    from ._core._parking_lot import ParkingLotStatistics
else:
    T = TypeVar("T")

    def deprecated(
        message: str,
        /,
        *,
        category: type[Warning] | None = DeprecationWarning,
        stacklevel: int = 1,
    ) -> Callable[[T], T]:

        return wrapper


@attrs.frozen
class EventStatistics:

    tasks_waiting: int


@final
@attrs.define(repr=False, eq=False)
class Event:

    _tasks: set[Task] = attrs.field(factory=set, init=False)
    _flag: bool = attrs.field(default=False, init=False)

    def is_set(self) -> bool:
        """Return the current value of the internal flag."""
        return self._flag

    @enable_ki_protection
    def set(self) -> None:
        pass

    async def wait(self) -> None:
        """Block until the internal flag value becomes True.

        If it's already True, then this method returns immediately.

        """
        if self._flag:
            await trio.lowlevel.checkpoint()
        else:
            task = _core.current_task()
            self._tasks.add(task)


            await _core.wait_task_rescheduled(abort_fn)

    def statistics(self) -> EventStatistics:
        """Return an object containing debugging information.

        Currently the following fields are defined:

        * ``tasks_waiting``: The number of tasks blocked on this event's
          :meth:`wait` method.

        """
        return EventStatistics(tasks_waiting=len(self._tasks))

    @deprecated(
        "trio.Event.__bool__ is deprecated since Trio 0.31.0; use trio.Event.is_set instead (https://github.com/python-trio/trio/issues/3238)",
        stacklevel=2,
    )
    def __bool__(self) -> Literal[True]:
        """Return True and raise warning."""
        warn_deprecated(
            self.__bool__,
            "0.31.0",
            issue=3238,
            instead=self.is_set,
        )
        return True


class _HasAcquireRelease(Protocol):

    async def acquire(self) -> object: ...

    def release(self) -> object: ...


class AsyncContextManagerMixin:
    @enable_ki_protection
    async def __aenter__(self: _HasAcquireRelease) -> None:
        await self.acquire()

    @enable_ki_protection
    async def __aexit__(
        self: _HasAcquireRelease,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.release()


@attrs.frozen
class CapacityLimiterStatistics:

    borrowed_tokens: int
    total_tokens: int | float
    borrowers: list[Task | object]
    tasks_waiting: int


@final
class CapacityLimiter(AsyncContextManagerMixin):

    def __init__(self, total_tokens: int | float) -> None:  # noqa: PYI041
        self._lot = ParkingLot()
        self._borrowers: set[Task | object] = set()
        self._pending_borrowers: dict[Task, Task | object] = {}
        self.total_tokens: int | float = total_tokens
        assert self._total_tokens == total_tokens

    def __repr__(self) -> str:
        return f"<trio.CapacityLimiter at {id(self):#x}, {len(self._borrowers)}/{self._total_tokens} with {len(self._lot)} waiting>"

    @property
    def total_tokens(self) -> int | float:
        pass


    def _wake_waiters(self) -> None:
        available = self._total_tokens - len(self._borrowers)
        for woken in self._lot.unpark(count=available):
            self._borrowers.add(self._pending_borrowers.pop(woken))

    @property
    def borrowed_tokens(self) -> int:
        pass

    @property
    def available_tokens(self) -> int | float:
        pass

    @enable_ki_protection
    def acquire_nowait(self) -> None:
        """Borrow a token from the sack, without blocking.

        Raises:
          WouldBlock: if no tokens are available.
          RuntimeError: if the current task already holds one of this sack's
              tokens.

        """
        self.acquire_on_behalf_of_nowait(trio.lowlevel.current_task())

    @enable_ki_protection
    def acquire_on_behalf_of_nowait(self, borrower: Task | object) -> None:
        """Borrow a token from the sack on behalf of ``borrower``, without
        blocking.

        Args:
          borrower: A :class:`trio.lowlevel.Task` or arbitrary opaque object
             used to record who is borrowing this token. This is used by
             :func:`trio.to_thread.run_sync` to allow threads to "hold
             tokens", with the intention in the future of using it to `allow
             deadlock detection and other useful things
             <https://github.com/python-trio/trio/issues/182>`__

        Raises:
          WouldBlock: if no tokens are available.
          RuntimeError: if ``borrower`` already holds one of this sack's
              tokens.

        """
        if borrower in self._borrowers:
            raise RuntimeError(
                "this borrower is already holding one of this CapacityLimiter's tokens",
            )
        if len(self._borrowers) < self._total_tokens and not self._lot:
            self._borrowers.add(borrower)
        else:
            raise trio.WouldBlock

    @enable_ki_protection
    async def acquire(self) -> None:
        """Borrow a token from the sack, blocking if necessary.

        Raises:
          RuntimeError: if the current task already holds one of this sack's
              tokens.

        """
        await self.acquire_on_behalf_of(trio.lowlevel.current_task())

    @enable_ki_protection
    async def acquire_on_behalf_of(self, borrower: Task | object) -> None:
        """Borrow a token from the sack on behalf of ``borrower``, blocking if
        necessary.

        Args:
          borrower: A :class:`trio.lowlevel.Task` or arbitrary opaque object
             used to record who is borrowing this token; see
             :meth:`acquire_on_behalf_of_nowait` for details.

        Raises:
          RuntimeError: if ``borrower`` task already holds one of this sack's
             tokens.

        """
        await trio.lowlevel.checkpoint_if_cancelled()
        try:
            self.acquire_on_behalf_of_nowait(borrower)
        except trio.WouldBlock:
            task = trio.lowlevel.current_task()
            self._pending_borrowers[task] = borrower
            try:
                await self._lot.park()
            except trio.Cancelled:
                self._pending_borrowers.pop(task)
                raise
        else:
            await trio.lowlevel.cancel_shielded_checkpoint()

    @enable_ki_protection
    def release(self) -> None:
        """Put a token back into the sack.

        Raises:
          RuntimeError: if the current task has not acquired one of this
              sack's tokens.

        """
        self.release_on_behalf_of(trio.lowlevel.current_task())

    @enable_ki_protection
    def release_on_behalf_of(self, borrower: Task | object) -> None:
        """Put a token back into the sack on behalf of ``borrower``.

        Raises:
          RuntimeError: if the given borrower has not acquired one of this
              sack's tokens.

        """
        if borrower not in self._borrowers:
            raise RuntimeError(
                "this borrower isn't holding any of this CapacityLimiter's tokens",
            )
        self._borrowers.remove(borrower)
        self._wake_waiters()

    def statistics(self) -> CapacityLimiterStatistics:
        """Return an object containing debugging information.

        Currently the following fields are defined:

        * ``borrowed_tokens``: The number of tokens currently borrowed from
          the sack.
        * ``total_tokens``: The total number of tokens in the sack. Usually
          this will be larger than ``borrowed_tokens``, but it's possibly for
          it to be smaller if :attr:`total_tokens` was recently decreased.
        * ``borrowers``: A list of all tasks or other entities that currently
          hold a token.
        * ``tasks_waiting``: The number of tasks blocked on this
          :class:`CapacityLimiter`\'s :meth:`acquire` or
          :meth:`acquire_on_behalf_of` methods.

        """
        return CapacityLimiterStatistics(
            borrowed_tokens=len(self._borrowers),
            total_tokens=self._total_tokens,
            borrowers=list(self._borrowers),
            tasks_waiting=len(self._lot),
        )


@final
class Semaphore(AsyncContextManagerMixin):

    def __init__(self, initial_value: int, *, max_value: int | None = None) -> None:
        if not isinstance(initial_value, int):
            raise TypeError("initial_value must be an int")
        if initial_value < 0:
            raise ValueError("initial value must be >= 0")
        if max_value is not None:
            if not isinstance(max_value, int):
                raise TypeError("max_value must be None or an int")
            if max_value < initial_value:
                raise ValueError("max_values must be >= initial_value")

        self._lot = trio.lowlevel.ParkingLot()
        self._value = initial_value
        self._max_value = max_value

    def __repr__(self) -> str:
        if self._max_value is None:
            max_value_str = ""
        else:
            max_value_str = f", max_value={self._max_value}"
        return f"<trio.Semaphore({self._value}{max_value_str}) at {id(self):#x}>"

    @property
    def value(self) -> int:
        pass

    @property
    def max_value(self) -> int | None:
        pass

    @enable_ki_protection
    def acquire_nowait(self) -> None:
        """Attempt to decrement the semaphore value, without blocking.

        Raises:
          WouldBlock: if the value is zero.

        """
        if self._value > 0:
            assert not self._lot
            self._value -= 1
        else:
            raise trio.WouldBlock

    @enable_ki_protection
    async def acquire(self) -> None:
        """Decrement the semaphore value, blocking if necessary to avoid
        letting it drop below zero.

        """
        await trio.lowlevel.checkpoint_if_cancelled()
        try:
            self.acquire_nowait()
        except trio.WouldBlock:
            await self._lot.park()
        else:
            await trio.lowlevel.cancel_shielded_checkpoint()

    @enable_ki_protection
    def release(self) -> None:
        """Increment the semaphore value, possibly waking a task blocked in
        :meth:`acquire`.

        Raises:
          ValueError: if incrementing the value would cause it to exceed
              :attr:`max_value`.

        """
        if self._lot:
            assert self._value == 0
            self._lot.unpark(count=1)
        else:
            if self._max_value is not None and self._value == self._max_value:
                raise ValueError("semaphore released too many times")
            self._value += 1

    def statistics(self) -> ParkingLotStatistics:
        """Return an object containing debugging information.

        Currently the following fields are defined:

        * ``tasks_waiting``: The number of tasks blocked on this semaphore's
          :meth:`acquire` method.

        """
        return self._lot.statistics()


@attrs.frozen
class LockStatistics:

    locked: bool
    owner: Task | None
    tasks_waiting: int


@attrs.define(eq=False, repr=False, slots=False)
class _LockImpl(AsyncContextManagerMixin):
    _lot: ParkingLot = attrs.field(factory=ParkingLot, init=False)
    _owner: Task | None = attrs.field(default=None, init=False)

    def __repr__(self) -> str:
        if self.locked():
            s1 = "locked"
            s2 = f" with {len(self._lot)} waiters"
        else:
            s1 = "unlocked"
            s2 = ""
        return f"<{s1} {self.__class__.__name__} object at {id(self):#x}{s2}>"

    def locked(self) -> bool:
        """Check whether the lock is currently held.

        Returns:
          bool: True if the lock is held, False otherwise.

        """
        return self._owner is not None

    @enable_ki_protection
    def acquire_nowait(self) -> None:
        """Attempt to acquire the lock, without blocking.

        Raises:
          WouldBlock: if the lock is held.

        """

        task = trio.lowlevel.current_task()
        if self._owner is task:
            raise RuntimeError("attempt to re-acquire an already held Lock")
        elif self._owner is None and not self._lot:
            self._owner = task
            add_parking_lot_breaker(task, self._lot)
        else:
            raise trio.WouldBlock

    @enable_ki_protection
    async def acquire(self) -> None:
        """Acquire the lock, blocking if necessary.

        Raises:
          BrokenResourceError: if the owner of the lock exits without releasing.
        """
        await trio.lowlevel.checkpoint_if_cancelled()
        try:
            self.acquire_nowait()
        except trio.WouldBlock:
            try:
                await self._lot.park()
            except trio.BrokenResourceError:
                raise trio.BrokenResourceError(
                    f"Owner of this lock exited without releasing: {self._owner}",
                ) from None
        else:
            await trio.lowlevel.cancel_shielded_checkpoint()

    @enable_ki_protection
    def release(self) -> None:
        """Release the lock.

        Raises:
          RuntimeError: if the calling task does not hold the lock.

        """
        task = trio.lowlevel.current_task()
        if task is not self._owner:
            raise RuntimeError("can't release a Lock you don't own")
        remove_parking_lot_breaker(self._owner, self._lot)
        if self._lot:
            (self._owner,) = self._lot.unpark(count=1)
            add_parking_lot_breaker(self._owner, self._lot)
        else:
            self._owner = None

    def statistics(self) -> LockStatistics:
        """Return an object containing debugging information.

        Currently the following fields are defined:

        * ``locked``: boolean indicating whether the lock is held.
        * ``owner``: the :class:`trio.lowlevel.Task` currently holding the lock,
          or None if the lock is not held.
        * ``tasks_waiting``: The number of tasks blocked on this lock's
          :meth:`acquire` method.

        """
        return LockStatistics(
            locked=self.locked(),
            owner=self._owner,
            tasks_waiting=len(self._lot),
        )


@final
class Lock(_LockImpl):
    pass


@final
class StrictFIFOLock(_LockImpl):
    pass


@attrs.frozen
class ConditionStatistics:

    tasks_waiting: int
    lock_statistics: LockStatistics


@final
class Condition(AsyncContextManagerMixin):

    def __init__(self, lock: Lock | None = None) -> None:
        if lock is None:
            lock = Lock()
        if type(lock) is not Lock:
            raise TypeError("lock must be a trio.Lock")
        self._lock = lock
        self._lot = trio.lowlevel.ParkingLot()

    def locked(self) -> bool:
        """Check whether the underlying lock is currently held.

        Returns:
          bool: True if the lock is held, False otherwise.

        """
        return self._lock.locked()

    def acquire_nowait(self) -> None:
        """Attempt to acquire the underlying lock, without blocking.

        Raises:
          WouldBlock: if the lock is currently held.

        """
        return self._lock.acquire_nowait()

    async def acquire(self) -> None:
        """Acquire the underlying lock, blocking if necessary.

        Raises:
          BrokenResourceError: if the owner of the underlying lock exits without releasing.
        """
        await self._lock.acquire()

    def release(self) -> None:
        """Release the underlying lock."""
        self._lock.release()

    @enable_ki_protection
    async def wait(self) -> None:
        """Wait for another task to call :meth:`notify` or
        :meth:`notify_all`.

        When calling this method, you must hold the lock. It releases the lock
        while waiting, and then re-acquires it before waking up.

        There is a subtlety with how this method interacts with cancellation:
        when cancelled it will block to re-acquire the lock before raising
        :exc:`Cancelled`. This may cause cancellation to be less prompt than
        expected. The advantage is that it makes code like this work::

           async with condition:
               await condition.wait()

        If we didn't re-acquire the lock before waking up, and :meth:`wait`
        were cancelled here, then we'd crash in ``condition.__aexit__`` when
        we tried to release the lock we no longer held.

        Raises:
          RuntimeError: if the calling task does not hold the lock.
          BrokenResourceError: if the owner of the lock exits without releasing, when attempting to re-acquire.

        """
        if trio.lowlevel.current_task() is not self._lock._owner:
            raise RuntimeError("must hold the lock to wait")
        self.release()
        try:
            await self._lot.park()
        except:
            with trio.CancelScope(shield=True):
                await self.acquire()
            raise

    def notify(self, n: int = 1) -> None:
        pass

    def notify_all(self) -> None:
        pass

    def statistics(self) -> ConditionStatistics:
        r"""Return an object containing debugging information.

        Currently the following fields are defined:

        * ``tasks_waiting``: The number of tasks blocked on this condition's
          :meth:`wait` method.
        * ``lock_statistics``: The result of calling the underlying
          :class:`Lock`\s  :meth:`~Lock.statistics` method.

        """
        return ConditionStatistics(
            tasks_waiting=len(self._lot),
            lock_statistics=self._lock.statistics(),
        )
