from __future__ import annotations

from contextlib import AbstractContextManager, contextmanager
from typing import TYPE_CHECKING

from .. import _core

if TYPE_CHECKING:
    from collections.abc import Generator


@contextmanager
def _assert_yields_or_not(expected: bool) -> Generator[None, None, None]:
    """Check if checkpoints are executed in a block of code."""
    __tracebackhide__ = True
    task = _core.current_task()
    orig_cancel = task._cancel_points
    orig_schedule = task._schedule_points
    try:
        yield
        if expected and (
            task._cancel_points == orig_cancel or task._schedule_points == orig_schedule
        ):
            raise AssertionError("assert_checkpoints block did not yield!")
    finally:
        if not expected and (
            task._cancel_points != orig_cancel or task._schedule_points != orig_schedule
        ):
            raise AssertionError("assert_no_checkpoints block yielded!")


def assert_checkpoints() -> AbstractContextManager[None]:
    """Use as a context manager to check that the code inside the ``with``
    block either exits with an exception or executes at least one
    :ref:`checkpoint <checkpoints>`.

    Raises:
      AssertionError: if no checkpoint was executed.

    Example:
      Check that :func:`trio.sleep` is a checkpoint, even if it doesn't
      block::

         with trio.testing.assert_checkpoints():
             await trio.sleep(0)

    """
    __tracebackhide__ = True
    return _assert_yields_or_not(True)


def assert_no_checkpoints() -> AbstractContextManager[None]:
    pass
