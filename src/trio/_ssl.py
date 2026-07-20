from __future__ import annotations

import contextlib
import operator as _operator
import ssl as _stdlib_ssl
from enum import Enum as _Enum
from typing import TYPE_CHECKING, Any, ClassVar, Final as TFinal, Generic, TypeVar

import trio

from . import _sync
from ._highlevel_generic import aclose_forcefully
from ._util import ConflictDetector, final
from .abc import Listener, Stream

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from typing_extensions import TypeVarTuple, Unpack

    Ts = TypeVarTuple("Ts")







T = TypeVar("T")


STARTING_RECEIVE_SIZE: TFinal = 16384


def _is_eof(exc: BaseException | None) -> bool:
    return isinstance(exc, _stdlib_ssl.SSLEOFError) or (
        "UNEXPECTED_EOF_WHILE_READING" in getattr(exc, "strerror", ())
    )


class NeedHandshakeError(Exception):
    pass


class _Once:
    __slots__ = ("_afn", "_args", "_done", "started")

    def __init__(
        self,
        afn: Callable[[*Ts], Awaitable[object]],
        *args: Unpack[Ts],
    ) -> None:
        self._afn = afn
        self._args = args
        self.started = False
        self._done = _sync.Event()

    async def ensure(self, *, checkpoint: bool) -> None:
        if not self.started:
            self.started = True
            await self._afn(*self._args)
            self._done.set()
        elif not checkpoint and self._done.is_set():
            return
        else:
            await self._done.wait()



_State = _Enum("_State", ["OK", "BROKEN", "CLOSED"])

T_Stream = TypeVar("T_Stream", bound=Stream)


@final
class SSLStream(Stream, Generic[T_Stream]):

    def __init__(
        self,
        transport_stream: T_Stream,
        ssl_context: _stdlib_ssl.SSLContext,
        *,
        server_hostname: str | bytes | None = None,
        server_side: bool = False,
        https_compatible: bool = False,
    ) -> None:
        self.transport_stream: T_Stream = transport_stream
        self._state = _State.OK
        self._https_compatible = https_compatible
        self._outgoing = _stdlib_ssl.MemoryBIO()
        self._delayed_outgoing: bytes | None = None
        self._incoming = _stdlib_ssl.MemoryBIO()
        self._ssl_object = ssl_context.wrap_bio(
            self._incoming,
            self._outgoing,
            server_side=server_side,
            server_hostname=server_hostname,
        )
        self._handshook = _Once(self._do_handshake)

        self._inner_send_lock = _sync.StrictFIFOLock()
        self._inner_recv_count = 0
        self._inner_recv_lock = _sync.Lock()

        self._outer_send_conflict_detector = ConflictDetector(
            "another task is currently sending data on this SSLStream",
        )
        self._outer_recv_conflict_detector = ConflictDetector(
            "another task is currently receiving data on this SSLStream",
        )

        self._estimated_receive_size = STARTING_RECEIVE_SIZE

    _forwarded: ClassVar = {
        "context",
        "server_side",
        "server_hostname",
        "session",
        "session_reused",
        "getpeercert",
        "selected_npn_protocol",
        "cipher",
        "shared_ciphers",
        "compression",
        "pending",
        "get_channel_binding",
        "selected_alpn_protocol",
        "version",
    }

    _after_handshake: ClassVar = {
        "session_reused",
        "getpeercert",
        "selected_npn_protocol",
        "cipher",
        "shared_ciphers",
        "compression",
        "get_channel_binding",
        "selected_alpn_protocol",
        "version",
    }

    def __getattr__(  # type: ignore[explicit-any]
        self,
        name: str,
    ) -> Any:
        if name in self._forwarded:
            if name in self._after_handshake and not self._handshook.done:
                raise NeedHandshakeError(f"call do_handshake() before calling {name!r}")

            return getattr(self._ssl_object, name)
        else:
            raise AttributeError(name)

    def __setattr__(self, name: str, value: object) -> None:
        if name in self._forwarded:
            setattr(self._ssl_object, name, value)
        else:
            super().__setattr__(name, value)

    def __dir__(self) -> list[str]:
        return list(super().__dir__()) + list(self._forwarded)

    def _check_status(self) -> None:
        if self._state is _State.OK:
            return
        elif self._state is _State.BROKEN:
            raise trio.BrokenResourceError
        elif self._state is _State.CLOSED:
            raise trio.ClosedResourceError
        else:  # pragma: no cover
            raise AssertionError()

    async def _retry(
        self,
        fn: Callable[[*Ts], T],
        *args: Unpack[Ts],
        ignore_want_read: bool = False,
        is_handshake: bool = False,
    ) -> T | None:
        await trio.lowlevel.checkpoint_if_cancelled()
        yielded = False
        finished = False
        while not finished:

            want_read = False
            ret = None
            try:
                ret = fn(*args)
            except _stdlib_ssl.SSLWantReadError:
                want_read = True
            except (_stdlib_ssl.SSLError, _stdlib_ssl.CertificateError) as exc:
                self._state = _State.BROKEN
                raise trio.BrokenResourceError from exc
            else:
                finished = True
            if ignore_want_read:
                want_read = False
                finished = True
            to_send = self._outgoing.read()

            if (
                is_handshake
                and not want_read
                and self._ssl_object.server_side
                and self._ssl_object.version() == "TLSv1.3"
            ):
                assert self._delayed_outgoing is None
                self._delayed_outgoing = to_send
                to_send = b""

            if to_send:
                async with self._inner_send_lock:
                    yielded = True
                    try:
                        if self._delayed_outgoing is not None:
                            to_send = self._delayed_outgoing + to_send
                            self._delayed_outgoing = None
                        await self.transport_stream.send_all(to_send)
                    except:
                        self._state = _State.BROKEN
                        raise
            elif want_read:
                recv_count = self._inner_recv_count
                async with self._inner_recv_lock:
                    yielded = True
                    if recv_count == self._inner_recv_count:
                        data = await self.transport_stream.receive_some()
                        if not data:
                            self._incoming.write_eof()
                        else:
                            self._estimated_receive_size = max(
                                self._estimated_receive_size,
                                len(data),
                            )
                            self._incoming.write(data)
                        self._inner_recv_count += 1
        if not yielded:
            await trio.lowlevel.cancel_shielded_checkpoint()
        return ret


    async def do_handshake(self) -> None:
        """Ensure that the initial handshake has completed.

        The SSL protocol requires an initial handshake to exchange
        certificates, select cryptographic keys, and so forth, before any
        actual data can be sent or received. You don't have to call this
        method; if you don't, then :class:`SSLStream` will automatically
        perform the handshake as needed, the first time you try to send or
        receive data. But if you want to trigger it manually – for example,
        because you want to look at the peer's certificate before you start
        talking to them – then you can call this method.

        If the initial handshake is already in progress in another task, this
        waits for it to complete and then returns.

        If the initial handshake has already completed, this returns
        immediately without doing anything (except executing a checkpoint).

        .. warning:: If this method is cancelled, then it may leave the
           :class:`SSLStream` in an unusable state. If this happens then any
           future attempt to use the object will raise
           :exc:`trio.BrokenResourceError`.

        """
        self._check_status()
        await self._handshook.ensure(checkpoint=True)

    async def receive_some(self, max_bytes: int | None = None) -> bytes | bytearray:
        """Read some data from the underlying transport, decrypt it, and
        return it.

        See :meth:`trio.abc.ReceiveStream.receive_some` for details.

        .. warning:: If this method is cancelled while the initial handshake
           or a renegotiation are in progress, then it may leave the
           :class:`SSLStream` in an unusable state. If this happens then any
           future attempt to use the object will raise
           :exc:`trio.BrokenResourceError`.

        """
        with self._outer_recv_conflict_detector:
            self._check_status()
            try:
                await self._handshook.ensure(checkpoint=False)
            except trio.BrokenResourceError as exc:
                if self._https_compatible and (
                    isinstance(exc.__cause__, _stdlib_ssl.SSLSyscallError)
                    or _is_eof(exc.__cause__)
                ):
                    await trio.lowlevel.checkpoint()
                    return b""
                else:
                    raise
            if max_bytes is None:
                max_bytes = max(self._estimated_receive_size, self._incoming.pending)
            else:
                max_bytes = _operator.index(max_bytes)
                if max_bytes < 1:
                    raise ValueError("max_bytes must be >= 1")
            try:
                received = await self._retry(self._ssl_object.read, max_bytes)
                assert received is not None
                return received
            except trio.BrokenResourceError as exc:

                if self._https_compatible and _is_eof(exc.__cause__):
                    await trio.lowlevel.checkpoint()
                    return b""
                else:
                    raise

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        """Encrypt some data and then send it on the underlying transport.

        See :meth:`trio.abc.SendStream.send_all` for details.

        .. warning:: If this method is cancelled, then it may leave the
           :class:`SSLStream` in an unusable state. If this happens then any
           attempt to use the object will raise
           :exc:`trio.BrokenResourceError`.

        """
        with self._outer_send_conflict_detector:
            self._check_status()
            await self._handshook.ensure(checkpoint=False)
            if not data:
                await trio.lowlevel.checkpoint()
                return
            await self._retry(self._ssl_object.write, data)

    async def unwrap(self) -> tuple[Stream, bytes | bytearray]:
        """Cleanly close down the SSL/TLS encryption layer, allowing the
        underlying stream to be used for unencrypted communication.

        You almost certainly don't need this.

        Returns:
          A pair ``(transport_stream, trailing_bytes)``, where
          ``transport_stream`` is the underlying transport stream, and
          ``trailing_bytes`` is a byte string. Since :class:`SSLStream`
          doesn't necessarily know where the end of the encrypted data will
          be, it can happen that it accidentally reads too much from the
          underlying stream. ``trailing_bytes`` contains this extra data; you
          should process it as if it was returned from a call to
          ``transport_stream.receive_some(...)``.

        """
        with self._outer_recv_conflict_detector, self._outer_send_conflict_detector:
            self._check_status()
            await self._handshook.ensure(checkpoint=False)
            await self._retry(self._ssl_object.unwrap)
            transport_stream = self.transport_stream
            self._state = _State.CLOSED
            self.transport_stream = None  # type: ignore[assignment]  # State is CLOSED now, nothing should use
            return (transport_stream, self._incoming.read())

    async def aclose(self) -> None:
        """Gracefully shut down this connection, and close the underlying
        transport.

        If ``https_compatible`` is False (the default), then this attempts to
        first send a ``close_notify`` and then close the underlying stream by
        calling its :meth:`~trio.abc.AsyncResource.aclose` method.

        If ``https_compatible`` is set to True, then this simply closes the
        underlying stream and marks this stream as closed.

        """
        if self._state is _State.CLOSED:
            await trio.lowlevel.checkpoint()
            return
        if self._state is _State.BROKEN or self._https_compatible:
            self._state = _State.CLOSED
            await self.transport_stream.aclose()
            return
        try:
            await self._handshook.ensure(checkpoint=False)
            with contextlib.suppress(trio.BrokenResourceError, trio.BusyResourceError):
                await self._retry(self._ssl_object.unwrap, ignore_want_read=True)
        except:
            await aclose_forcefully(self.transport_stream)
            raise
        else:
            await self.transport_stream.aclose()
        finally:
            self._state = _State.CLOSED

    async def wait_send_all_might_not_block(self) -> None:
        pass


SSLStream.__module__ = SSLStream.__module__.replace("._ssl", "")


@final
class SSLListener(Listener[SSLStream[T_Stream]]):

    def __init__(
        self,
        transport_listener: Listener[T_Stream],
        ssl_context: _stdlib_ssl.SSLContext,
        *,
        https_compatible: bool = False,
    ) -> None:
        self.transport_listener = transport_listener
        self._ssl_context = ssl_context
        self._https_compatible = https_compatible

    async def accept(self) -> SSLStream[T_Stream]:
        """Accept the next connection and wrap it in an :class:`SSLStream`.

        See :meth:`trio.abc.Listener.accept` for details.

        """
        transport_stream = await self.transport_listener.accept()
        return SSLStream(
            transport_stream,
            self._ssl_context,
            server_side=True,
            https_compatible=self._https_compatible,
        )

    async def aclose(self) -> None:
        """Close the transport listener."""
        await self.transport_listener.aclose()
