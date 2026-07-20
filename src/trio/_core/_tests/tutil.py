from __future__ import annotations

import asyncio
import gc
import os
import socket as stdlib_socket
import sys
import warnings
from contextlib import closing, contextmanager
from typing import TYPE_CHECKING, TypeVar

import pytest

from trio._tests.pytest_plugin import RUN_SLOW

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Sequence

slow = pytest.mark.skipif(not RUN_SLOW, reason="use --run-slow to run slow tests")

T = TypeVar("T")

try:
    s = stdlib_socket.socket(stdlib_socket.AF_INET6, stdlib_socket.SOCK_STREAM, 0)
except OSError:  # pragma: no cover
    can_create_ipv6 = False
    can_bind_ipv6 = False
else:
    can_create_ipv6 = True
    with s:
        try:
            s.bind(("::1", 0))
        except OSError:  # pragma: no cover # since support for 3.7 was removed
            can_bind_ipv6 = False
        else:
            can_bind_ipv6 = True

creates_ipv6 = pytest.mark.skipif(not can_create_ipv6, reason="need IPv6")
binds_ipv6 = pytest.mark.skipif(not can_bind_ipv6, reason="need IPv6")


def gc_collect_harder() -> None:
    for _ in range(5):
        gc.collect()




def _noop(*args: object, **kwargs: object) -> None:
    pass  # pragma: no cover


@contextmanager
def restore_unraisablehook() -> Generator[None, None, None]:
    sys.unraisablehook, prev = sys.__unraisablehook__, sys.unraisablehook
    try:
        yield
    finally:
        sys.unraisablehook = prev




skip_if_fbsd_pipes_broken = pytest.mark.skipif(
    sys.platform != "win32"  # prevent mypy from complaining about missing uname
    and hasattr(os, "uname")
    and os.uname().sysname == "FreeBSD"
    and os.uname().release[:4] < "12.2",
    reason="hangs on FreeBSD 12.1 and earlier, due to FreeBSD bug #246350",
)


