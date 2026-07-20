from __future__ import annotations

import ctypes
import ctypes.util
import os
import sys
import traceback
from contextvars import Context
from functools import partial
from itertools import count
from threading import Lock, Thread
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import outcome

if TYPE_CHECKING:
    from collections.abc import Callable

RetT = TypeVar("RetT")


def _to_os_thread_name(name: str) -> bytes:
    return name.encode("ascii", errors="replace")[:15]


def get_os_thread_name_func() -> Callable[[int | None, str], None] | None:


    libpthread_path = ctypes.util.find_library("pthread")
    if not libpthread_path:
        libpthread_path = "libc.so"

    try:
        libpthread = ctypes.CDLL(libpthread_path)
    except Exception:  # pragma: no cover
        return None

    pthread_setname_np = getattr(libpthread, "pthread_setname_np", None)
    if pthread_setname_np is None:  # pragma: no cover
        return None

    pthread_setname_np.restype = ctypes.c_int

    if sys.platform == "darwin":
        pthread_setname_np.argtypes = [ctypes.c_char_p]
        return partial(darwin_namefunc, pthread_setname_np)

    pthread_setname_np.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    return partial(namefunc, pthread_setname_np)


set_os_thread_name = get_os_thread_name_func()


IDLE_TIMEOUT = 10  # seconds

name_counter = count()


class WorkerThread(Generic[RetT]):
    __slots__ = ("_default_name", "_job", "_thread", "_thread_cache", "_worker_lock")

    def __init__(self, thread_cache: ThreadCache) -> None:
        self._job: (
            tuple[
                Callable[[], RetT],
                Callable[[outcome.Outcome[RetT]], object],
                str | None,
            ]
            | None
        ) = None
        self._thread_cache = thread_cache
        self._worker_lock = Lock()
        self._worker_lock.acquire()
        self._default_name = f"Trio thread {next(name_counter)}"
        if sys.version_info >= (3, 14):
            self._thread = Thread(
                target=self._work,
                name=self._default_name,
                daemon=True,
                context=Context(),
            )
        else:
            self._thread = Thread(
                target=self._work, name=self._default_name, daemon=True
            )

        if set_os_thread_name:
            set_os_thread_name(self._thread.ident, self._default_name)
        self._thread.start()




class ThreadCache:
    __slots__ = ("_idle_workers",)

    def __init__(self) -> None:
        self._idle_workers: dict[WorkerThread[Any], None] = {}  # type: ignore[explicit-any]

    def start_thread_soon(
        self,
        fn: Callable[[], RetT],
        deliver: Callable[[outcome.Outcome[RetT]], object],
        name: str | None = None,
    ) -> None:
        worker: WorkerThread[RetT]
        try:
            worker, _ = self._idle_workers.popitem()
        except KeyError:
            worker = WorkerThread(self)
        worker._job = (fn, deliver, name)
        worker._worker_lock.release()


THREAD_CACHE = ThreadCache()


def start_thread_soon(
    fn: Callable[[], RetT],
    deliver: Callable[[outcome.Outcome[RetT]], object],
    name: str | None = None,
) -> None:
    """Runs ``deliver(outcome.capture(fn))`` in a worker thread.

    Generally ``fn`` does some blocking work, and ``deliver`` delivers the
    result back to whoever is interested.

    This is a low-level, no-frills interface, very similar to using
    `threading.Thread` to spawn a thread directly. The main difference is
    that this function tries to reuse threads when possible, so it can be
    a bit faster than `threading.Thread`.

    Worker threads have the `~threading.Thread.daemon` flag set, which means
    that if your main thread exits, worker threads will automatically be
    killed. If you want to make sure that your ``fn`` runs to completion, then
    you should make sure that the main thread remains alive until ``deliver``
    is called.

    It is safe to call this function simultaneously from multiple threads.

    Args:

        fn (sync function): Performs arbitrary blocking work.

        deliver (sync function): Takes the `outcome.Outcome` of ``fn``, and
          delivers it. *Must not block.*

    Because worker threads are cached and reused for multiple calls, neither
    function should mutate thread-level state, like `threading.local` objects
    – or if they do, they should be careful to revert their changes before
    returning.

    Note:

        The split between ``fn`` and ``deliver`` serves two purposes. First,
        it's convenient, since most callers need something like this anyway.

        Second, it avoids a small race condition that could cause too many
        threads to be spawned. Consider a program that wants to run several
        jobs sequentially on a thread, so the main thread submits a job, waits
        for it to finish, submits another job, etc. In theory, this program
        should only need one worker thread. But what could happen is:

        1. Worker thread: First job finishes, and calls ``deliver``.

        2. Main thread: receives notification that the job finished, and calls
           ``start_thread_soon``.

        3. Main thread: sees that no worker threads are marked idle, so spawns
           a second worker thread.

        4. Original worker thread: marks itself as idle.

        To avoid this, threads mark themselves as idle *before* calling
        ``deliver``.

        Is this potential extra thread a major problem? Maybe not, but it's
        easy enough to avoid, and we figure that if the user is trying to
        limit how many threads they're using then it's polite to respect that.

    """
    THREAD_CACHE.start_thread_soon(fn, deliver, name)




if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=clear_worker_threads)
