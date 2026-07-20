from __future__ import annotations

import errno
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, overload

import trio

from . import socket as tsocket
from ._util import ConflictDetector, final
from .abc import HalfCloseableStream, Listener

if TYPE_CHECKING:
    from collections.abc import Generator

    from ._socket import SocketType

import sys

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
elif TYPE_CHECKING:
    from typing_extensions import Buffer

DEFAULT_RECEIVE_SIZE = 65536

_closed_stream_errnos = {
    errno.EBADF,
    errno.ENOTSOCK,
}


@contextmanager
def _translate_socket_errors_to_stream_errors() -> Generator[None, None, None]:
    try:
        yield
    except OSError as exc:
        if exc.errno in _closed_stream_errnos:
            raise trio.ClosedResourceError("this socket was already closed") from None
        else:
            raise trio.BrokenResourceError(f"socket connection broken: {exc}") from exc


@final
class SocketStream(HalfCloseableStream):

    def __init__(self, socket: SocketType) -> None:
        if not isinstance(socket, tsocket.SocketType):
            raise TypeError("SocketStream requires a Trio socket object")
        if socket.type != tsocket.SOCK_STREAM:
            raise ValueError("SocketStream requires a SOCK_STREAM socket")

        self.socket = socket
        self._send_conflict_detector = ConflictDetector(
            "another task is currently sending data on this SocketStream",
        )


        with suppress(OSError):
            self.setsockopt(tsocket.IPPROTO_TCP, tsocket.TCP_NODELAY, True)

        if hasattr(tsocket, "TCP_NOTSENT_LOWAT"):
            with suppress(OSError):
                self.setsockopt(tsocket.IPPROTO_TCP, tsocket.TCP_NOTSENT_LOWAT, 2**14)

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        if self.socket.did_shutdown_SHUT_WR:
            raise trio.ClosedResourceError("can't send data after sending EOF")
        with self._send_conflict_detector:
            with _translate_socket_errors_to_stream_errors():
                with memoryview(data) as data:
                    if not data:
                        if self.socket.fileno() == -1:
                            raise trio.ClosedResourceError("socket was already closed")
                        await trio.lowlevel.checkpoint()
                        return
                    total_sent = 0
                    while total_sent < len(data):
                        with data[total_sent:] as remaining:
                            sent = await self.socket.send(remaining)
                        total_sent += sent



    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        if max_bytes is None:
            max_bytes = DEFAULT_RECEIVE_SIZE
        if max_bytes < 1:
            raise ValueError("max_bytes must be >= 1")
        with _translate_socket_errors_to_stream_errors():
            return await self.socket.recv(max_bytes)

    async def aclose(self) -> None:
        self.socket.close()
        await trio.lowlevel.checkpoint()


    @overload
    def setsockopt(self, level: int, option: int, value: int | Buffer) -> None: ...

    @overload
    def setsockopt(self, level: int, option: int, value: None, length: int) -> None: ...

    def setsockopt(
        self,
        level: int,
        option: int,
        value: int | Buffer | None,
        length: int | None = None,
    ) -> None:
        """Set an option on the underlying socket.

        See :meth:`socket.socket.setsockopt` for details.

        """
        if length is None:
            if value is None:
                raise TypeError(
                    "invalid value for argument 'value', must not be None when specifying length",
                )
            return self.socket.setsockopt(level, option, value)
        if value is not None:
            raise TypeError(
                f"invalid value for argument 'value': {value!r}, must be None when specifying optlen",
            )
        return self.socket.setsockopt(level, option, value, length)

    @overload
    def getsockopt(self, level: int, option: int) -> int: ...

    @overload
    def getsockopt(self, level: int, option: int, buffersize: int) -> bytes: ...

    def getsockopt(self, level: int, option: int, buffersize: int = 0) -> int | bytes:
        """Check the current value of an option on the underlying socket.

        See :meth:`socket.socket.getsockopt` for details.

        """
        if buffersize == 0:
            return self.socket.getsockopt(level, option)
        else:
            return self.socket.getsockopt(level, option, buffersize)




_ignorable_accept_errno_names = [
    "EPERM",
    "ECONNABORTED",
    "EPROTO",
    "ENETDOWN",
    "ENOPROTOOPT",
    "EHOSTDOWN",
    "ENONET",
    "EHOSTUNREACH",
    "EOPNOTSUPP",
    "ENETUNREACH",
    "ENOSR",
    "ESOCKTNOSUPPORT",
    "EPROTONOSUPPORT",
    "ETIMEDOUT",
    "ECONNRESET",
]

_ignorable_accept_errnos: set[int] = set()
for name in _ignorable_accept_errno_names:
    with suppress(AttributeError):
        _ignorable_accept_errnos.add(getattr(errno, name))


@final
class SocketListener(Listener[SocketStream]):

    def __init__(self, socket: SocketType) -> None:
        if not isinstance(socket, tsocket.SocketType):
            raise TypeError("SocketListener requires a Trio socket object")
        if socket.type != tsocket.SOCK_STREAM:
            raise ValueError("SocketListener requires a SOCK_STREAM socket")
        try:
            listening = socket.getsockopt(tsocket.SOL_SOCKET, tsocket.SO_ACCEPTCONN)
        except OSError:
            pass
        else:
            if not listening:
                raise ValueError("SocketListener requires a listening socket")

        self.socket = socket

    async def accept(self) -> SocketStream:
        """Accept an incoming connection.

        Returns:
          :class:`SocketStream`

        Raises:
          OSError: if the underlying call to ``accept`` raises an unexpected
              error.
          ClosedResourceError: if you already closed the socket.

        This method handles routine errors like ``ECONNABORTED``, but passes
        other errors on to its caller. In particular, it does *not* make any
        special effort to handle resource exhaustion errors like ``EMFILE``,
        ``ENFILE``, ``ENOBUFS``, ``ENOMEM``.

        """
        while True:
            try:
                sock, _ = await self.socket.accept()
            except OSError as exc:
                if exc.errno in _closed_stream_errnos:
                    raise trio.ClosedResourceError from None
                if exc.errno not in _ignorable_accept_errnos:
                    raise
            else:
                return SocketStream(sock)

    async def aclose(self) -> None:
        """Close this listener and its underlying socket."""
        self.socket.close()
        await trio.lowlevel.checkpoint()
