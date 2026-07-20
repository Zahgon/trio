from __future__ import annotations

import sys
from collections import OrderedDict, deque
from collections.abc import AsyncGenerator, Callable  # noqa: TC003  # Needed for Sphinx
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from functools import wraps
from math import inf
from typing import (
    TYPE_CHECKING,
    Generic,
)

import attrs
from outcome import Error, Value

import trio

from ._abc import ReceiveChannel, ReceiveType, SendChannel, SendType, T
from ._core import Abort, BrokenResourceError, RaiseCancelT, Task, enable_ki_protection
from ._util import (
    MultipleExceptionError,
    NoPublicConstructor,
    final,
    raise_single_exception_from_group,
)

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import ParamSpec, Self

    P = ParamSpec("P")
elif "sphinx.ext.autodoc" in sys.modules:
    try:
        from typing_extensions import ParamSpec
    except ImportError:
        P = ...  # This is valid in Callable, though not correct
    else:
        P = ParamSpec("P")


@final
class open_memory_channel(tuple["MemorySendChannel[T]", "MemoryReceiveChannel[T]"]):

    def __new__(  # type: ignore[misc]  # "must return a subtype"
        cls,
        max_buffer_size: int | float,  # noqa: PYI041
    ) -> tuple[MemorySendChannel[T], MemoryReceiveChannel[T]]:
        if max_buffer_size != inf and not isinstance(max_buffer_size, int):
            raise TypeError("max_buffer_size must be an integer or math.inf")
        if max_buffer_size < 0:
            raise ValueError("max_buffer_size must be >= 0")
        state: MemoryChannelState[T] = MemoryChannelState(max_buffer_size)
        return (
            MemorySendChannel[T]._create(state),
            MemoryReceiveChannel[T]._create(state),
        )

    def __init__(self, max_buffer_size: int | float) -> None:  # noqa: PYI041
        ...


@attrs.frozen
class MemoryChannelStatistics:

    current_buffer_used: int
    """The number of items currently stored in the channel buffer."""

    max_buffer_size: int | float
    """The maximum number of items that can be buffered in the channel."""

    open_send_channels: int
    """The number of open :class:`MemorySendChannel` endpoints pointing to this channel."""

    open_receive_channels: int
    """The number of open :class:`MemoryReceiveChannel` endpoints pointing to this channel."""

    tasks_waiting_send: int
    """The number of tasks currently blocked waiting to send."""

    tasks_waiting_receive: int
    """The number of tasks currently blocked waiting to receive."""


@attrs.define
class MemoryChannelState(Generic[T]):
    max_buffer_size: int | float
    data: deque[T] = attrs.Factory(deque)
    open_send_channels: int = 0
    open_receive_channels: int = 0
    send_tasks: OrderedDict[Task, T] = attrs.Factory(OrderedDict)
    receive_tasks: OrderedDict[Task, None] = attrs.Factory(OrderedDict)

    def statistics(self) -> MemoryChannelStatistics:
        return MemoryChannelStatistics(
            current_buffer_used=len(self.data),
            max_buffer_size=self.max_buffer_size,
            open_send_channels=self.open_send_channels,
            open_receive_channels=self.open_receive_channels,
            tasks_waiting_send=len(self.send_tasks),
            tasks_waiting_receive=len(self.receive_tasks),
        )


@final
@attrs.define(eq=False, repr=False, slots=False)
class MemorySendChannel(SendChannel[SendType], metaclass=NoPublicConstructor):

    _state: MemoryChannelState[SendType]
    _closed: bool = False
    _tasks: set[Task] = attrs.Factory(set)

    def __attrs_post_init__(self) -> None:
        self._state.open_send_channels += 1

    def __repr__(self) -> str:
        return f"<send channel at {id(self):#x}, using buffer at {id(self._state):#x}>"

    def statistics(self) -> MemoryChannelStatistics:
        """Returns a `MemoryChannelStatistics` for the memory channel this is
        associated with."""
        return self._state.statistics()

    @enable_ki_protection
    def send_nowait(self, value: SendType) -> None:
        """Like `~trio.abc.SendChannel.send`, but if the channel's buffer is
        full, raises `WouldBlock` instead of blocking.

        """
        if self._closed:
            raise trio.ClosedResourceError
        if self._state.open_receive_channels == 0:
            raise trio.BrokenResourceError
        if self._state.receive_tasks:
            assert not self._state.data
            task, _ = self._state.receive_tasks.popitem(last=False)
            task.custom_sleep_data._tasks.remove(task)
            trio.lowlevel.reschedule(task, Value(value))
        elif len(self._state.data) < self._state.max_buffer_size:
            self._state.data.append(value)
        else:
            raise trio.WouldBlock

    @enable_ki_protection
    async def send(self, value: SendType) -> None:
        """See `SendChannel.send <trio.abc.SendChannel.send>`.

        Memory channels allow multiple tasks to call `send` at the same time.

        """
        await trio.lowlevel.checkpoint_if_cancelled()
        try:
            self.send_nowait(value)
        except trio.WouldBlock:
            pass
        else:
            await trio.lowlevel.cancel_shielded_checkpoint()
            return

        task = trio.lowlevel.current_task()
        self._tasks.add(task)
        self._state.send_tasks[task] = value
        task.custom_sleep_data = self


        await trio.lowlevel.wait_task_rescheduled(abort_fn)

    @enable_ki_protection
    def clone(self) -> MemorySendChannel[SendType]:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @enable_ki_protection
    def close(self) -> None:
        """Close this send channel object synchronously.

        All channel objects have an asynchronous `~.AsyncResource.aclose` method.
        Memory channels can also be closed synchronously. This has the same
        effect on the channel and other tasks using it, but `close` is not a
        trio checkpoint. This simplifies cleaning up in cancelled tasks.

        Using ``with send_channel:`` will close the channel object on leaving
        the with block.

        """
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            trio.lowlevel.reschedule(task, Error(trio.ClosedResourceError()))
            del self._state.send_tasks[task]
        self._tasks.clear()
        self._state.open_send_channels -= 1
        if self._state.open_send_channels == 0:
            assert not self._state.send_tasks
            for task in self._state.receive_tasks:
                task.custom_sleep_data._tasks.remove(task)
                trio.lowlevel.reschedule(task, Error(trio.EndOfChannel()))
            self._state.receive_tasks.clear()

    @enable_ki_protection
    async def aclose(self) -> None:
        """Close this send channel object asynchronously.

        See `MemorySendChannel.close`."""
        self.close()
        await trio.lowlevel.checkpoint()


@final
@attrs.define(eq=False, repr=False, slots=False)
class MemoryReceiveChannel(ReceiveChannel[ReceiveType], metaclass=NoPublicConstructor):

    _state: MemoryChannelState[ReceiveType]
    _closed: bool = False
    _tasks: set[trio._core._run.Task] = attrs.Factory(set)

    def __attrs_post_init__(self) -> None:
        self._state.open_receive_channels += 1

    def statistics(self) -> MemoryChannelStatistics:
        """Returns a `MemoryChannelStatistics` for the memory channel this is
        associated with."""
        return self._state.statistics()

    def __repr__(self) -> str:
        return (
            f"<receive channel at {id(self):#x}, using buffer at {id(self._state):#x}>"
        )

    @enable_ki_protection
    def receive_nowait(self) -> ReceiveType:
        """Like `~trio.abc.ReceiveChannel.receive`, but if there's nothing
        ready to receive, raises `WouldBlock` instead of blocking.

        """
        if self._closed:
            raise trio.ClosedResourceError
        if self._state.send_tasks:
            task, value = self._state.send_tasks.popitem(last=False)
            task.custom_sleep_data._tasks.remove(task)
            trio.lowlevel.reschedule(task)
            self._state.data.append(value)
        if self._state.data:
            return self._state.data.popleft()
        if not self._state.open_send_channels:
            raise trio.EndOfChannel
        raise trio.WouldBlock

    @enable_ki_protection
    async def receive(self) -> ReceiveType:
        """See `ReceiveChannel.receive <trio.abc.ReceiveChannel.receive>`.

        Memory channels allow multiple tasks to call `receive` at the same
        time. The first task will get the first item sent, the second task
        will get the second item sent, and so on.

        """
        await trio.lowlevel.checkpoint_if_cancelled()
        try:
            value = self.receive_nowait()
        except trio.WouldBlock:
            pass
        else:
            await trio.lowlevel.cancel_shielded_checkpoint()
            return value

        task = trio.lowlevel.current_task()
        self._tasks.add(task)
        self._state.receive_tasks[task] = None
        task.custom_sleep_data = self


        return await trio.lowlevel.wait_task_rescheduled(abort_fn)  # type: ignore[no-any-return]

    @enable_ki_protection
    def clone(self) -> MemoryReceiveChannel[ReceiveType]:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @enable_ki_protection
    def close(self) -> None:
        """Close this receive channel object synchronously.

        All channel objects have an asynchronous `~.AsyncResource.aclose` method.
        Memory channels can also be closed synchronously. This has the same
        effect on the channel and other tasks using it, but `close` is not a
        trio checkpoint. This simplifies cleaning up in cancelled tasks.

        Using ``with receive_channel:`` will close the channel object on
        leaving the with block.

        """
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            trio.lowlevel.reschedule(task, Error(trio.ClosedResourceError()))
            del self._state.receive_tasks[task]
        self._tasks.clear()
        self._state.open_receive_channels -= 1
        if self._state.open_receive_channels == 0:
            assert not self._state.receive_tasks
            for task in self._state.send_tasks:
                task.custom_sleep_data._tasks.remove(task)
                trio.lowlevel.reschedule(task, Error(trio.BrokenResourceError()))
            self._state.send_tasks.clear()
            self._state.data.clear()

    @enable_ki_protection
    async def aclose(self) -> None:
        """Close this receive channel object asynchronously.

        See `MemoryReceiveChannel.close`."""
        self.close()
        await trio.lowlevel.checkpoint()


class RecvChanWrapper(ReceiveChannel[T]):
    def __init__(
        self, recv_chan: MemoryReceiveChannel[T], send_semaphore: trio.Semaphore
    ) -> None:
        self._recv_chan = recv_chan
        self._send_semaphore = send_semaphore

    async def receive(self) -> T:
        self._send_semaphore.release()
        return await self._recv_chan.receive()

    async def aclose(self) -> None:
        await self._recv_chan.aclose()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._recv_chan.close()


def as_safe_channel(
    fn: Callable[P, AsyncGenerator[T, None]],
) -> Callable[P, AbstractAsyncContextManager[ReceiveChannel[T]]]:
    """Decorate an async generator function to make it cancellation-safe.

    The ``yield`` keyword offers a very convenient way to write iterators...
    which makes it really unfortunate that async generators are so difficult
    to call correctly.  Yielding from the inside of a cancel scope or a nursery
    to the outside `violates structured concurrency <https://xkcd.com/292/>`_
    with consequences explained in :pep:`789`.  Even then, resource cleanup
    errors remain common (:pep:`533`) unless you wrap every call in
    :func:`~contextlib.aclosing`.

    This decorator gives you the best of both worlds: with careful exception
    handling and a background task we preserve structured concurrency by
    offering only the safe interface, and you can still write your iterables
    with the convenience of ``yield``.  For example::

        @as_safe_channel
        async def my_async_iterable(arg, *, kwarg=True):
            while ...:
                item = await ...
                yield item

        async with my_async_iterable(...) as recv_chan:
            async for item in recv_chan:
                ...

    While the combined async-with-async-for can be inconvenient at first,
    the context manager is indispensable for both correctness and for prompt
    cleanup of resources.
    """



    return context_manager
