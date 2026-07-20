from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from . import _core
from ._abc import ReceiveStream, SendStream
from ._core._windows_cffi import _handle, kernel32, raise_winerror
from ._util import ConflictDetector, final

assert sys.platform == "win32" or not TYPE_CHECKING

DEFAULT_RECEIVE_SIZE = 65536


class _HandleHolder:
    def __init__(self, handle: int) -> None:
        self.handle = -1
        if not isinstance(handle, int):
            raise TypeError("handle must be an int")
        self.handle = handle
        _core.register_with_iocp(self.handle)


    def close(self) -> None:
        if self.closed:
            return
        handle = self.handle
        self.handle = -1
        if not kernel32.CloseHandle(_handle(handle)):
            raise_winerror()

    def __del__(self) -> None:
        self.close()


@final
class PipeSendStream(SendStream):

    def __init__(self, handle: int) -> None:
        self._handle_holder = _HandleHolder(handle)
        self._conflict_detector = ConflictDetector(
            "another task is currently using this pipe",
        )

    async def send_all(self, data: bytes | bytearray | memoryview[int]) -> None:
        with self._conflict_detector:
            if self._handle_holder.closed:
                raise _core.ClosedResourceError("this pipe is already closed")

            if not data:
                await _core.checkpoint()
                return

            try:
                written = await _core.write_overlapped(self._handle_holder.handle, data)
            except BrokenPipeError as ex:
                raise _core.BrokenResourceError from ex
            assert written == len(data)


    def close(self) -> None:
        self._handle_holder.close()

    async def aclose(self) -> None:
        self.close()
        await _core.checkpoint()


@final
class PipeReceiveStream(ReceiveStream):

    def __init__(self, handle: int) -> None:
        self._handle_holder = _HandleHolder(handle)
        self._conflict_detector = ConflictDetector(
            "another task is currently using this pipe",
        )

    async def receive_some(self, max_bytes: int | None = None) -> bytes | bytearray:
        with self._conflict_detector:
            if self._handle_holder.closed:
                raise _core.ClosedResourceError("this pipe is already closed")

            if max_bytes is None:
                max_bytes = DEFAULT_RECEIVE_SIZE
            else:
                if not isinstance(max_bytes, int):
                    raise TypeError("max_bytes must be integer >= 1")
                if max_bytes < 1:
                    raise ValueError("max_bytes must be integer >= 1")

            buffer = bytearray(max_bytes)
            try:
                size = await _core.readinto_overlapped(
                    self._handle_holder.handle,
                    buffer,
                )
            except BrokenPipeError:
                if self._handle_holder.closed:
                    raise _core.ClosedResourceError(
                        "another task closed this pipe",
                    ) from None

                await _core.checkpoint()
                return b""
            else:
                del buffer[size:]
                return buffer

    def close(self) -> None:
        self._handle_holder.close()

    async def aclose(self) -> None:
        self.close()
        await _core.checkpoint()
