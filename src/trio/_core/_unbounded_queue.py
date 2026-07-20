from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

import attrs

from .. import _core
from .._deprecate import deprecated
from .._util import final

T = TypeVar("T")

if TYPE_CHECKING:
    from typing_extensions import Self


@attrs.frozen
class UnboundedQueueStatistics:

    qsize: int
    tasks_waiting: int


@final
class UnboundedQueue(Generic[T]):

    @deprecated(
        "0.9.0",
        issue=497,
        thing="trio.lowlevel.UnboundedQueue",
        instead="trio.open_memory_channel(math.inf)",
        use_triodeprecationwarning=True,
    )
    def __init__(self) -> None:
        self._lot = _core.ParkingLot()
        self._data: list[T] = []
        self._can_get = False

    def __repr__(self) -> str:
        return f"<UnboundedQueue holding {len(self._data)} items>"

    def qsize(self) -> int:
        pass

    def empty(self) -> bool:
        pass

    @_core.enable_ki_protection
    def put_nowait(self, obj: T) -> None:
        """Put an object into the queue, without blocking.

        This always succeeds, because the queue is unbounded. We don't provide
        a blocking ``put`` method, because it would never need to block.

        Args:
          obj (object): The object to enqueue.

        """
        if not self._data:
            assert not self._can_get
            if self._lot:
                self._lot.unpark(count=1)
            else:
                self._can_get = True
        self._data.append(obj)

    def _get_batch_protected(self) -> list[T]:
        data = self._data.copy()
        self._data.clear()
        self._can_get = False
        return data

    def get_batch_nowait(self) -> list[T]:
        pass

    async def get_batch(self) -> list[T]:
        """Get the next batch from the queue, blocking as necessary.

        Returns:
          list: A list of dequeued items, in order. This list is always
              non-empty.

        """
        await _core.checkpoint_if_cancelled()
        if not self._can_get:
            await self._lot.park()
            return self._get_batch_protected()
        else:
            try:
                return self._get_batch_protected()
            finally:
                await _core.cancel_shielded_checkpoint()

    def statistics(self) -> UnboundedQueueStatistics:
        """Return an :class:`UnboundedQueueStatistics` object containing debugging information."""
        return UnboundedQueueStatistics(
            qsize=len(self._data),
            tasks_waiting=self._lot.statistics().tasks_waiting,
        )

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> list[T]:
        return await self.get_batch()
