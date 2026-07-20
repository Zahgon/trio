from __future__ import annotations

import inspect
import math
from collections import OrderedDict
from typing import TYPE_CHECKING

import attrs
import outcome

from .. import _core
from .._util import final

if TYPE_CHECKING:
    from collections.abc import Iterator

    from ._run import Task


GLOBAL_PARKING_LOT_BREAKER: dict[Task, list[ParkingLot]] = {}


def add_parking_lot_breaker(task: Task, lot: ParkingLot) -> None:
    """Register a task as a breaker for a lot. See :meth:`ParkingLot.break_lot`.

    raises:
      trio.BrokenResourceError: if the task has already exited.
    """
    if inspect.getcoroutinestate(task.coro) == inspect.CORO_CLOSED:
        raise _core._exceptions.BrokenResourceError(
            "Attempted to add already exited task as lot breaker.",
        )
    if task not in GLOBAL_PARKING_LOT_BREAKER:
        GLOBAL_PARKING_LOT_BREAKER[task] = [lot]
    else:
        GLOBAL_PARKING_LOT_BREAKER[task].append(lot)


def remove_parking_lot_breaker(task: Task, lot: ParkingLot) -> None:
    """Deregister a task as a breaker for a lot. See :meth:`ParkingLot.break_lot`"""
    try:
        GLOBAL_PARKING_LOT_BREAKER[task].remove(lot)
    except (KeyError, ValueError):
        raise RuntimeError(
            "Attempted to remove task as breaker for a lot it is not registered for",
        ) from None
    if not GLOBAL_PARKING_LOT_BREAKER[task]:
        del GLOBAL_PARKING_LOT_BREAKER[task]


@attrs.frozen
class ParkingLotStatistics:

    tasks_waiting: int


@final
@attrs.define(eq=False)
class ParkingLot:

    _parked: OrderedDict[Task, None] = attrs.field(factory=OrderedDict, init=False)
    broken_by: list[Task] = attrs.field(factory=list, init=False)

    def __len__(self) -> int:
        """Returns the number of parked tasks."""
        return len(self._parked)

    def __bool__(self) -> bool:
        """True if there are parked tasks, False otherwise."""
        return bool(self._parked)

    @_core.enable_ki_protection
    async def park(self) -> None:
        """Park the current task until woken by a call to :meth:`unpark` or
        :meth:`unpark_all`.

        Raises:
          BrokenResourceError: if attempting to park in a broken lot, or the lot
            breaks before we get to unpark.

        """
        if self.broken_by:
            raise _core.BrokenResourceError(
                f"Attempted to park in parking lot broken by {self.broken_by}",
            )
        task = _core.current_task()
        self._parked[task] = None
        task.custom_sleep_data = self


        await _core.wait_task_rescheduled(abort_fn)

    def _pop_several(self, count: int | float) -> Iterator[Task]:  # noqa: PYI041
        if isinstance(count, float):
            if math.isinf(count):
                count = len(self._parked)
            else:
                raise ValueError("Cannot pop a non-integer number of tasks.")
        else:
            count = min(count, len(self._parked))
        for _ in range(count):
            task, _ = self._parked.popitem(last=False)
            yield task

    @_core.enable_ki_protection
    def unpark(self, *, count: int | float = 1) -> list[Task]:  # noqa: PYI041
        """Unpark one or more tasks.

        This wakes up ``count`` tasks that are blocked in :meth:`park`. If
        there are fewer than ``count`` tasks parked, then wakes as many tasks
        are available and then returns successfully.

        Args:
          count (int | math.inf): the number of tasks to unpark.

        """
        tasks = list(self._pop_several(count))
        for task in tasks:
            _core.reschedule(task)
        return tasks

    def unpark_all(self) -> list[Task]:
        """Unpark all parked tasks."""
        return self.unpark(count=len(self))

    @_core.enable_ki_protection
    def repark(
        self,
        new_lot: ParkingLot,
        *,
        count: int | float = 1,  # noqa: PYI041
    ) -> None:
        pass

    def repark_all(self, new_lot: ParkingLot) -> None:
        pass

    def break_lot(self, task: Task | None = None) -> None:
        """Break this lot, with ``task`` noted as the task that broke it.

        This causes all parked tasks to raise an error, and any
        future tasks attempting to park to error. Unpark & repark become no-ops as the
        parking lot is empty.

        The error raised contains a reference to the task sent as a parameter. The task
        is also saved in the parking lot in the ``broken_by`` attribute.
        """
        if task is None:
            task = _core.current_task()

        if self.broken_by:
            self.broken_by.append(task)
            return

        self.broken_by.append(task)

        for parked_task in self._parked:
            _core.reschedule(
                parked_task,
                outcome.Error(
                    _core.BrokenResourceError(f"Parking lot broken by {task}"),
                ),
            )
        self._parked.clear()

    def statistics(self) -> ParkingLotStatistics:
        """Return an object containing debugging information.

        Currently the following fields are defined:

        * ``tasks_waiting``: The number of tasks blocked on this lot's
          :meth:`park` method.

        """
        return ParkingLotStatistics(tasks_waiting=len(self._parked))
