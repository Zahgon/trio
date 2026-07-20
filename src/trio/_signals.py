from __future__ import annotations

import signal
from collections import OrderedDict
from contextlib import contextmanager
from typing import TYPE_CHECKING

import trio

from ._util import ConflictDetector, is_main_thread

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Generator, Iterable
    from types import FrameType

    from typing_extensions import Self





class SignalReceiver:
    def __init__(self) -> None:
        self._pending: OrderedDict[int, None] = OrderedDict()
        self._lot = trio.lowlevel.ParkingLot()
        self._conflict_detector = ConflictDetector(
            "only one task can iterate on a signal receiver at a time",
        )
        self._closed = False



    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> int:
        if self._closed:
            raise RuntimeError("open_signal_receiver block already exited")
        with self._conflict_detector:
            if not self._pending:
                await self._lot.park()
            else:
                await trio.lowlevel.checkpoint()
            signum, _ = self._pending.popitem(last=False)
            return signum


def get_pending_signal_count(rec: AsyncIterator[int]) -> int:
    pass


@contextmanager
def open_signal_receiver(
    *signals: signal.Signals | int,
) -> Generator[AsyncIterator[int], None, None]:
    pass
