from __future__ import annotations

import ssl
from typing import TYPE_CHECKING, NoReturn, TypeVar

import trio

from ._highlevel_open_tcp_stream import DEFAULT_DELAY

T = TypeVar("T")

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from ._highlevel_socket import SocketStream


async def open_ssl_over_tcp_stream(
    host: str | bytes,
    port: int,
    *,
    https_compatible: bool = False,
    ssl_context: ssl.SSLContext | None = None,
    happy_eyeballs_delay: float | None = DEFAULT_DELAY,
) -> trio.SSLStream[SocketStream]:
    pass


async def open_ssl_over_tcp_listeners(
    port: int,
    ssl_context: ssl.SSLContext,
    *,
    host: str | bytes | None = None,
    https_compatible: bool = False,
    backlog: int | None = None,
) -> list[trio.SSLListener[SocketStream]]:
    pass


async def serve_ssl_over_tcp(
    handler: Callable[[trio.SSLStream[SocketStream]], Awaitable[object]],
    port: int,
    ssl_context: ssl.SSLContext,
    *,
    host: str | bytes | None = None,
    https_compatible: bool = False,
    backlog: int | None = None,
    handler_nursery: trio.Nursery | None = None,
    task_status: trio.TaskStatus[
        list[trio.SSLListener[SocketStream]]
    ] = trio.TASK_STATUS_IGNORED,
) -> NoReturn:
    pass
