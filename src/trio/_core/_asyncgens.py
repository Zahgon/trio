from __future__ import annotations

import logging
import sys
import warnings
import weakref
from typing import TYPE_CHECKING, NoReturn, TypeVar

import attrs

from .. import _core
from .._util import name_asyncgen
from . import _run

ASYNCGEN_LOGGER = logging.getLogger("trio.async_generator_errors")

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import AsyncGeneratorType

    from typing_extensions import ParamSpec

    _P = ParamSpec("_P")

    _WEAK_ASYNC_GEN_SET = weakref.WeakSet[AsyncGeneratorType[object, NoReturn]]
    _ASYNC_GEN_SET = set[AsyncGeneratorType[object, NoReturn]]
else:
    _WEAK_ASYNC_GEN_SET = weakref.WeakSet
    _ASYNC_GEN_SET = set

_R = TypeVar("_R")


@_core.disable_ki_protection
def _call_without_ki_protection(
    f: Callable[_P, _R],
    /,
    *args: _P.args,
    **kwargs: _P.kwargs,
) -> _R:
    return f(*args, **kwargs)


@attrs.define(eq=False)
class AsyncGenerators:
    alive: _WEAK_ASYNC_GEN_SET | _ASYNC_GEN_SET = attrs.Factory(_WEAK_ASYNC_GEN_SET)
    foreign: set[int] = attrs.Factory(set)

    trailing_needs_finalize: _ASYNC_GEN_SET = attrs.Factory(_ASYNC_GEN_SET)

    prev_hooks: sys._asyncgen_hooks = attrs.field(init=False)

    def install_hooks(self, runner: _run.Runner) -> None:
        def firstiter(agen: AsyncGeneratorType[object, NoReturn]) -> None:
            if hasattr(_run.GLOBAL_RUN_CONTEXT, "task"):
                self.alive.add(agen)
            else:
                self.foreign.add(id(agen))
                if self.prev_hooks.firstiter is not None:
                    self.prev_hooks.firstiter(agen)



        self.prev_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(firstiter=firstiter, finalizer=finalizer)  # type: ignore[arg-type]  # Finalizer doesn't use AsyncGeneratorType


    def close(self) -> None:
        sys.set_asyncgen_hooks(*self.prev_hooks)

