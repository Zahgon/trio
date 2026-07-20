from __future__ import annotations

import errno
import logging
import os
from collections.abc import Awaitable, Callable
from typing import Any, NoReturn, TypeVar

import trio

ACCEPT_CAPACITY_ERRNOS = {
    errno.EMFILE,
    errno.ENFILE,
    errno.ENOMEM,
    errno.ENOBUFS,
}

SLEEP_TIME = 0.100

LOGGER = logging.getLogger("trio.serve_listeners")


StreamT = TypeVar("StreamT", bound=trio.abc.AsyncResource)
ListenerT = TypeVar("ListenerT", bound=trio.abc.Listener[Any])  # type: ignore[explicit-any]
Handler = Callable[[StreamT], Awaitable[object]]








async def serve_listeners(  # type: ignore[explicit-any]
    handler: Handler[StreamT],
    listeners: list[ListenerT],
    *,
    handler_nursery: trio.Nursery | None = None,
    task_status: trio.TaskStatus[list[ListenerT]] = trio.TASK_STATUS_IGNORED,
) -> NoReturn:
    r"""Listen for incoming connections on ``listeners``, and for each one
    start a task running ``handler(stream)``.

    .. warning::

       If ``handler`` raises an exception, then this function doesn't do
       anything special to catch it – so by default the exception will
       propagate out and crash your server. If you don't want this, then catch
       exceptions inside your ``handler``, or use a ``handler_nursery`` object
       that responds to exceptions in some other way.

    Args:

      handler: An async callable, that will be invoked like
          ``handler_nursery.start_soon(handler, stream)`` for each incoming
          connection.

      listeners: A list of :class:`~trio.abc.Listener` objects.
          :func:`serve_listeners` takes responsibility for closing them.

      handler_nursery: The nursery used to start handlers, or any object with
          a ``start_soon`` method. If ``None`` (the default), then
          :func:`serve_listeners` will create a new nursery internally and use
          that.

      task_status: This function can be used with ``nursery.start``, which
          will return ``listeners``.

    Returns:

      This function never returns unless cancelled.

    Resource handling:

      If ``handler`` neglects to close the ``stream``, then it will be closed
      using :func:`trio.aclose_forcefully`.

    Error handling:

      Most errors coming from :meth:`~trio.abc.Listener.accept` are allowed to
      propagate out (crashing the server in the process). However, some errors –
      those which indicate that the server is temporarily overloaded – are
      handled specially. These are :class:`OSError`\s with one of the following
      errnos:

      * ``EMFILE``: process is out of file descriptors
      * ``ENFILE``: system is out of file descriptors
      * ``ENOBUFS``, ``ENOMEM``: the kernel hit some sort of memory limitation
        when trying to create a socket object

      When :func:`serve_listeners` gets one of these errors, then it:

      * Logs the error to the standard library logger ``trio.serve_listeners``
        (level = ERROR, with exception information included). By default this
        causes it to be printed to stderr.
      * Waits 100 ms before calling ``accept`` again, in hopes that the
        system will recover.

    """
    async with trio.open_nursery() as nursery:
        if handler_nursery is None:
            handler_nursery = nursery
        for listener in listeners:
            nursery.start_soon(_serve_one_listener, listener, handler_nursery, handler)
        task_status.started(listeners)

    raise AssertionError(
        "_serve_one_listener should never complete",
    )  # pragma: no cover
