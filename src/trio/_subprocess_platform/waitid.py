import errno
import math
import os
import sys
from typing import TYPE_CHECKING

from .. import _core, _subprocess
from .._sync import CapacityLimiter, Event
from .._threads import to_thread_run_sync

assert (sys.platform != "win32" and sys.platform != "darwin") or not TYPE_CHECKING

try:
    from os import waitid


except ImportError:
    import cffi

    waitid_ffi = cffi.FFI()

    waitid_ffi.cdef(
        """
typedef struct siginfo_s {
    int si_signo;
    int si_errno;
    int si_code;
    int si_pid;
    int si_uid;
    int si_status;
    int pad[26];
} siginfo_t;
int waitid(int idtype, int id, siginfo_t* result, int options);
""",
    )
    waitid_cffi = waitid_ffi.dlopen(None).waitid  # type: ignore[attr-defined]




waitid_limiter = CapacityLimiter(math.inf)


async def _waitid_system_task(pid: int, event: Event) -> None:
    pass


async def wait_child_exiting(process: "_subprocess.Process") -> None:

    if process._wait_for_exit_data is None:
        process._wait_for_exit_data = event = Event()
        _core.spawn_system_task(_waitid_system_task, process.pid, event)
    assert isinstance(process._wait_for_exit_data, Event)
    await process._wait_for_exit_data.wait()
