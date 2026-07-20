from __future__ import annotations

from collections import defaultdict
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import attrs

from .. import Event, _core, _util

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@_util.final
@attrs.define(eq=False, slots=False)
class Sequencer:

    _sequence_points: defaultdict[int, Event] = attrs.field(
        factory=lambda: defaultdict(Event),
        init=False,
    )
    _claimed: set[int] = attrs.field(factory=set, init=False)
    _broken: bool = attrs.field(default=False, init=False)

    @asynccontextmanager
    async def __call__(self, position: int) -> AsyncIterator[None]:
        if position in self._claimed:
            raise RuntimeError(f"Attempted to reuse sequence point {position}")
        if self._broken:
            raise RuntimeError("sequence broken!")
        self._claimed.add(position)
        if position != 0:
            try:
                await self._sequence_points[position].wait()
            except _core.Cancelled:
                self._broken = True
                for event in self._sequence_points.values():
                    event.set()
                raise RuntimeError(
                    "Sequencer wait cancelled -- sequence broken",
                ) from None
            else:
                if self._broken:
                    raise RuntimeError("sequence broken!")
        try:
            yield
        finally:
            self._sequence_points[position + 1].set()
