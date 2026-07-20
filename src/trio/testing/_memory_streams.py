from __future__ import annotations

import operator
from collections.abc import Awaitable, Callable
from typing import TypeAlias, TypeVar

from .. import _core, _util
from .._highlevel_generic import StapledStream
from ..abc import ReceiveStream, SendStream

AsyncHook: TypeAlias = Callable[[], Awaitable[object]]
SyncHook: TypeAlias = Callable[[], object]
SendStreamT = TypeVar("SendStreamT", bound=SendStream)
ReceiveStreamT = TypeVar("ReceiveStreamT", bound=ReceiveStream)




class _UnboundedByteQueue:
    def __init__(self) -> None:
        self._data = bytearray()
        self._closed = False
        self._lot = _core.ParkingLot()
        self._fetch_lock = _util.ConflictDetector(
            "another task is already fetching data",
        )

    def close(self) -> None:
        self._closed = True
        self._lot.unpark_all()

    def close_and_wipe(self) -> None:
        self._data = bytearray()
        self.close()

    def put(self, data: bytes | bytearray | memoryview) -> None:
        if self._closed:
            raise _core.ClosedResourceError("virtual connection closed")
        self._data += data
        self._lot.unpark_all()

    def _check_max_bytes(self, max_bytes: int | None) -> None:
        if max_bytes is None:
            return
        max_bytes = operator.index(max_bytes)
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")

    def _get_impl(self, max_bytes: int | None) -> bytearray:
        assert self._closed or self._data
        if max_bytes is None:
            max_bytes = len(self._data)
        if self._data:
            chunk = self._data[:max_bytes]
            del self._data[:max_bytes]
            assert chunk
            return chunk
        else:
            return bytearray()

    def get_nowait(self, max_bytes: int | None = None) -> bytearray:
        with self._fetch_lock:
            self._check_max_bytes(max_bytes)
            if not self._closed and not self._data:
                raise _core.WouldBlock
            return self._get_impl(max_bytes)

    async def get(self, max_bytes: int | None = None) -> bytearray:
        with self._fetch_lock:
            self._check_max_bytes(max_bytes)
            if not self._closed and not self._data:
                await self._lot.park()
            else:
                await _core.checkpoint()
            return self._get_impl(max_bytes)


@_util.final
class MemorySendStream(SendStream):

    def __init__(
        self,
        send_all_hook: AsyncHook | None = None,
        wait_send_all_might_not_block_hook: AsyncHook | None = None,
        close_hook: SyncHook | None = None,
    ) -> None:
        self._conflict_detector = _util.ConflictDetector(
            "another task is using this stream",
        )
        self._outgoing = _UnboundedByteQueue()
        self.send_all_hook = send_all_hook
        self.wait_send_all_might_not_block_hook = wait_send_all_might_not_block_hook
        self.close_hook = close_hook

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        """Places the given data into the object's internal buffer, and then
        calls the :attr:`send_all_hook` (if any).

        """
        with self._conflict_detector:
            await _core.checkpoint()
            await _core.checkpoint()
            self._outgoing.put(data)
            if self.send_all_hook is not None:
                await self.send_all_hook()

    async def wait_send_all_might_not_block(self) -> None:
        pass

    def close(self) -> None:
        """Marks this stream as closed, and then calls the :attr:`close_hook`
        (if any).

        """
        self._outgoing.close()
        if self.close_hook is not None:
            self.close_hook()

    async def aclose(self) -> None:
        """Same as :meth:`close`, but async."""
        self.close()
        await _core.checkpoint()

    async def get_data(self, max_bytes: int | None = None) -> bytearray:
        pass

    def get_data_nowait(self, max_bytes: int | None = None) -> bytearray:
        """Retrieves data from the internal buffer, but doesn't block.

        See :meth:`get_data` for details.

        Raises:
          trio.WouldBlock: if no data is available to retrieve.

        """
        return self._outgoing.get_nowait(max_bytes)


@_util.final
class MemoryReceiveStream(ReceiveStream):

    def __init__(
        self,
        receive_some_hook: AsyncHook | None = None,
        close_hook: SyncHook | None = None,
    ) -> None:
        self._conflict_detector = _util.ConflictDetector(
            "another task is using this stream",
        )
        self._incoming = _UnboundedByteQueue()
        self._closed = False
        self.receive_some_hook = receive_some_hook
        self.close_hook = close_hook

    async def receive_some(self, max_bytes: int | None = None) -> bytearray:
        """Calls the :attr:`receive_some_hook` (if any), and then retrieves
        data from the internal buffer, blocking if necessary.

        """
        with self._conflict_detector:
            await _core.checkpoint()
            await _core.checkpoint()
            if self._closed:
                raise _core.ClosedResourceError
            if self.receive_some_hook is not None:
                await self.receive_some_hook()
            data = await self._incoming.get(max_bytes)
            if self._closed:
                raise _core.ClosedResourceError
            return data

    def close(self) -> None:
        """Discards any pending data from the internal buffer, and marks this
        stream as closed.

        """
        self._closed = True
        self._incoming.close_and_wipe()
        if self.close_hook is not None:
            self.close_hook()

    async def aclose(self) -> None:
        """Same as :meth:`close`, but async."""
        self.close()
        await _core.checkpoint()

    def put_data(self, data: bytes | bytearray | memoryview) -> None:
        """Appends the given data to the internal buffer."""
        self._incoming.put(data)

    def put_eof(self) -> None:
        pass


MemorySendStream.__module__ = MemorySendStream.__module__.replace(
    "._memory_streams", ""
)
MemoryReceiveStream.__module__ = MemoryReceiveStream.__module__.replace(
    "._memory_streams", ""
)


def memory_stream_pump(
    memory_send_stream: MemorySendStream,
    memory_receive_stream: MemoryReceiveStream,
    *,
    max_bytes: int | None = None,
) -> bool:
    pass


def memory_stream_one_way_pair() -> tuple[MemorySendStream, MemoryReceiveStream]:
    pass


def _make_stapled_pair(
    one_way_pair: Callable[[], tuple[SendStreamT, ReceiveStreamT]],
) -> tuple[
    StapledStream[SendStreamT, ReceiveStreamT],
    StapledStream[SendStreamT, ReceiveStreamT],
]:
    pipe1_send, pipe1_recv = one_way_pair()
    pipe2_send, pipe2_recv = one_way_pair()
    stream1 = StapledStream(pipe1_send, pipe2_recv)
    stream2 = StapledStream(pipe2_send, pipe1_recv)
    return stream1, stream2


def memory_stream_pair() -> tuple[
    StapledStream[MemorySendStream, MemoryReceiveStream],
    StapledStream[MemorySendStream, MemoryReceiveStream],
]:
    """Create a connected, pure-Python, bidirectional stream with infinite
    buffering and flexible configuration options.

    This is a convenience function that creates two one-way streams using
    :func:`memory_stream_one_way_pair`, and then uses
    :class:`~trio.StapledStream` to combine them into a single bidirectional
    stream.

    This is like a no-operating-system-involved, Trio-streamsified version of
    :func:`socket.socketpair`.

    Returns:
      A pair of :class:`~trio.StapledStream` objects that are connected so
      that data automatically flows from one to the other in both directions.

    After creating a stream pair, you can send data back and forth, which is
    enough for simple tests::

       left, right = memory_stream_pair()
       await left.send_all(b"123")
       assert await right.receive_some() == b"123"
       await right.send_all(b"456")
       assert await left.receive_some() == b"456"

    But if you read the docs for :class:`~trio.StapledStream` and
    :func:`memory_stream_one_way_pair`, you'll see that all the pieces
    involved in wiring this up are public APIs, so you can adjust to suit the
    requirements of your tests. For example, here's how to tweak a stream so
    that data flowing from left to right trickles in one byte at a time (but
    data flowing from right to left proceeds at full speed)::

        left, right = memory_stream_pair()
        async def trickle():
            # left is a StapledStream, and left.send_stream is a MemorySendStream
            # right is a StapledStream, and right.recv_stream is a MemoryReceiveStream
            while memory_stream_pump(left.send_stream, right.recv_stream, max_bytes=1):
                # Pause between each byte
                await trio.sleep(1)
        # Normally this send_all_hook calls memory_stream_pump directly without
        # passing in a max_bytes. We replace it with our custom version:
        left.send_stream.send_all_hook = trickle

    And here's a simple test using our modified stream objects::

        async def sender():
            await left.send_all(b"12345")
            await left.send_eof()

        async def receiver():
            async for data in right:
                print(data)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(sender)
            nursery.start_soon(receiver)

    By default, this will print ``b"12345"`` and then immediately exit; with
    our trickle stream it instead sleeps 1 second, then prints ``b"1"``, then
    sleeps 1 second, then prints ``b"2"``, etc.

    Pro-tip: you can insert sleep calls (like in our example above) to
    manipulate the flow of data across tasks... and then use
    :class:`MockClock` and its :attr:`~MockClock.autojump_threshold`
    functionality to keep your test suite running quickly.

    If you want to stress test a protocol implementation, one nice trick is to
    use the :mod:`random` module (preferably with a fixed seed) to move random
    numbers of bytes at a time, and insert random sleeps in between them. You
    can also set up a custom :attr:`~MemoryReceiveStream.receive_some_hook` if
    you want to manipulate things on the receiving side, and not just the
    sending side.

    """
    return _make_stapled_pair(memory_stream_one_way_pair)




class _LockstepByteQueue:
    def __init__(self) -> None:
        self._data = bytearray()
        self._sender_closed = False
        self._receiver_closed = False
        self._receiver_waiting = False
        self._waiters = _core.ParkingLot()
        self._send_conflict_detector = _util.ConflictDetector(
            "another task is already sending",
        )
        self._receive_conflict_detector = _util.ConflictDetector(
            "another task is already receiving",
        )

    def _something_happened(self) -> None:
        self._waiters.unpark_all()

    async def _wait_for(self, fn: Callable[[], bool]) -> None:
        while True:
            if fn():
                break
            if self._sender_closed or self._receiver_closed:
                break
            await self._waiters.park()
        await _core.checkpoint()

    def close_sender(self) -> None:
        self._sender_closed = True
        self._something_happened()

    def close_receiver(self) -> None:
        self._receiver_closed = True
        self._something_happened()

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        with self._send_conflict_detector:
            if self._sender_closed:
                raise _core.ClosedResourceError
            if self._receiver_closed:
                raise _core.BrokenResourceError
            assert not self._data
            self._data += data
            self._something_happened()
            await self._wait_for(lambda: self._data == b"")
            if self._sender_closed:
                raise _core.ClosedResourceError
            if self._data and self._receiver_closed:
                raise _core.BrokenResourceError


    async def receive_some(self, max_bytes: int | None = None) -> bytes | bytearray:
        with self._receive_conflict_detector:
            if max_bytes is not None:
                max_bytes = operator.index(max_bytes)
                if max_bytes < 1:
                    raise ValueError("max_bytes must be >= 1")
            if self._receiver_closed:
                raise _core.ClosedResourceError
            self._receiver_waiting = True
            self._something_happened()
            try:
                await self._wait_for(lambda: self._data != b"")
            finally:
                self._receiver_waiting = False
            if self._receiver_closed:
                raise _core.ClosedResourceError
            if self._data:
                got = self._data[:max_bytes]
                del self._data[:max_bytes]
                self._something_happened()
                return got
            else:
                assert self._sender_closed
                return b""


class _LockstepSendStream(SendStream):
    def __init__(self, lbq: _LockstepByteQueue) -> None:
        self._lbq = lbq

    def close(self) -> None:
        self._lbq.close_sender()

    async def aclose(self) -> None:
        self.close()
        await _core.checkpoint()

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        await self._lbq.send_all(data)



class _LockstepReceiveStream(ReceiveStream):
    def __init__(self, lbq: _LockstepByteQueue) -> None:
        self._lbq = lbq

    def close(self) -> None:
        self._lbq.close_receiver()

    async def aclose(self) -> None:
        self.close()
        await _core.checkpoint()

    async def receive_some(self, max_bytes: int | None = None) -> bytes | bytearray:
        return await self._lbq.receive_some(max_bytes)


def lockstep_stream_one_way_pair() -> tuple[SendStream, ReceiveStream]:
    pass


def lockstep_stream_pair() -> tuple[
    StapledStream[SendStream, ReceiveStream],
    StapledStream[SendStream, ReceiveStream],
]:
    pass
