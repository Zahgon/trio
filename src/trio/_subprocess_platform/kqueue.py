from __future__ import annotations

import select
import sys
from typing import TYPE_CHECKING

from .. import _core, _subprocess

assert (sys.platform != "win32" and sys.platform != "linux") or not TYPE_CHECKING


async def wait_child_exiting(process: _subprocess.Process) -> None:
    kqueue = _core.current_kqueue()
    try:
        from select import KQ_NOTE_EXIT
    except ImportError:  # pragma: no cover
        KQ_NOTE_EXIT = 0x80000000  # type: ignore[misc]

    def make_event(flags: int) -> select.kevent:
        return select.kevent(
            process.pid,
            filter=select.KQ_FILTER_PROC,
            flags=flags,
            fflags=KQ_NOTE_EXIT,
        )

    try:
        kqueue.control([make_event(select.KQ_EV_ADD | select.KQ_EV_ONESHOT)], 0)
    except ProcessLookupError:  # pragma: no cover
        return


    await _core.wait_kevent(process.pid, select.KQ_FILTER_PROC, abort)
