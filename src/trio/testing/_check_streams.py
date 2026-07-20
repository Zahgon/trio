from __future__ import annotations

import random
import sys
from collections.abc import Awaitable, Callable, Generator
from contextlib import contextmanager, suppress
from typing import (
    TYPE_CHECKING,
    Generic,
    TypeAlias,
    TypeVar,
)

from .. import CancelScope, _core
from .._abc import AsyncResource, HalfCloseableStream, ReceiveStream, SendStream, Stream
from .._highlevel_generic import aclose_forcefully
from ._checkpoints import assert_checkpoints

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import ParamSpec

    ArgsT = ParamSpec("ArgsT")

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

Res1 = TypeVar("Res1", bound=AsyncResource)
Res2 = TypeVar("Res2", bound=AsyncResource)
StreamMaker: TypeAlias = Callable[[], Awaitable[tuple[Res1, Res2]]]


class _ForceCloseBoth(Generic[Res1, Res2]):
    def __init__(self, both: tuple[Res1, Res2]) -> None:
        self._first, self._second = both

    async def __aenter__(self) -> tuple[Res1, Res2]:
        return self._first, self._second

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await aclose_forcefully(self._first)
        finally:
            await aclose_forcefully(self._second)




async def check_one_way_stream(
    stream_maker: StreamMaker[SendStream, ReceiveStream],
    clogged_stream_maker: StreamMaker[SendStream, ReceiveStream] | None,
) -> None:
    pass


async def check_two_way_stream(
    stream_maker: StreamMaker[Stream, Stream],
    clogged_stream_maker: StreamMaker[Stream, Stream] | None,
) -> None:
    pass


async def check_half_closeable_stream(
    stream_maker: StreamMaker[HalfCloseableStream, HalfCloseableStream],
    clogged_stream_maker: StreamMaker[HalfCloseableStream, HalfCloseableStream] | None,
) -> None:
    pass
