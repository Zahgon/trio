
from typing import TYPE_CHECKING

from trio import TASK_STATUS_IGNORED, Nursery, TaskStatus

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable


async def task_0() -> None: ...


async def task_1a(value: int) -> None: ...


async def task_1b(value: str) -> None: ...


async def task_2a(a: int, b: str) -> None: ...


async def task_2b(a: str, b: int) -> None: ...


async def task_2c(a: str, b: int, optional: bool = False) -> None: ...


async def task_requires_kw(a: int, *, b: bool) -> None: ...


async def task_startable_1(
    a: str,
    *,
    task_status: TaskStatus[bool] = TASK_STATUS_IGNORED,
) -> None: ...


async def task_startable_2(
    a: str,
    b: float,
    *,
    task_status: TaskStatus[bool] = TASK_STATUS_IGNORED,
) -> None: ...


async def task_requires_start(*, task_status: TaskStatus[str]) -> None:
    """Check a function requiring start() to be used."""


async def task_pos_or_kw(value: str, task_status: TaskStatus[int]) -> None:
    """Check a function which doesn't use the *-syntax works."""


def check_start_soon(nursery: Nursery) -> None:
    pass
