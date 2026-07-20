from __future__ import annotations

import errno
import os
import sys
from typing import TYPE_CHECKING, Final as FinalType

import trio

from ._abc import Stream
from ._util import ConflictDetector, final

assert not TYPE_CHECKING or sys.platform != "win32"

DEFAULT_RECEIVE_SIZE: FinalType = 65536


class _FdHolder:
    fd: int

    def __init__(self, fd: int) -> None:
        self.fd = -1
        if not isinstance(fd, int):
            raise TypeError("file descriptor must be an int")
        self.fd = fd
        self._original_is_blocking = os.get_blocking(fd)
        os.set_blocking(fd, False)


    def _raw_close(self) -> None:
        if self.closed:
            return
        fd = self.fd
        self.fd = -1
        os.set_blocking(fd, self._original_is_blocking)
        os.close(fd)

    def __del__(self) -> None:
        self._raw_close()

    def close(self) -> None:
        if not self.closed:
            trio.lowlevel.notify_closing(self.fd)
            self._raw_close()


@final
class FdStream(Stream):

    def __init__(self, fd: int) -> None:
        self._fd_holder = _FdHolder(fd)
        self._send_conflict_detector = ConflictDetector(
            "another task is using this stream for send",
        )
        self._receive_conflict_detector = ConflictDetector(
            "another task is using this stream for receive",
        )

    async def send_all(self, data: bytes | bytearray | memoryview) -> None:
        with self._send_conflict_detector:
            if self._fd_holder.closed:
                raise trio.ClosedResourceError("file was already closed")
            await trio.lowlevel.checkpoint()
            length = len(data)
            with memoryview(data) as view:
                sent = 0
                while sent < length:
                    with view[sent:] as remaining:
                        try:
                            sent += os.write(self._fd_holder.fd, remaining)
                        except BlockingIOError:
                            await trio.lowlevel.wait_writable(self._fd_holder.fd)
                        except OSError as e:
                            if e.errno == errno.EBADF:
                                raise trio.ClosedResourceError(
                                    "file was already closed",
                                ) from None
                            else:
                                raise trio.BrokenResourceError from e


    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        with self._receive_conflict_detector:
            if max_bytes is None:
                max_bytes = DEFAULT_RECEIVE_SIZE
            else:
                if not isinstance(max_bytes, int):
                    raise TypeError("max_bytes must be integer >= 1")
                if max_bytes < 1:
                    raise ValueError("max_bytes must be integer >= 1")

            await trio.lowlevel.checkpoint()
            while True:
                try:
                    data = os.read(self._fd_holder.fd, max_bytes)
                except BlockingIOError:
                    await trio.lowlevel.wait_readable(self._fd_holder.fd)
                except OSError as exc:
                    if exc.errno == errno.EBADF:
                        raise trio.ClosedResourceError(
                            "file was already closed",
                        ) from None
                    else:
                        raise trio.BrokenResourceError from exc
                else:
                    break

            return data

    def close(self) -> None:
        self._fd_holder.close()

    async def aclose(self) -> None:
        self.close()
        await trio.lowlevel.checkpoint()

    def fileno(self) -> int:
        return self._fd_holder.fd
