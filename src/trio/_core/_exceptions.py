from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Literal, TypeAlias

import attrs

from trio._util import NoPublicConstructor, final

if TYPE_CHECKING:
    from collections.abc import Callable

    from typing_extensions import Self

CancelReasonLiteral: TypeAlias = Literal[
    "KeyboardInterrupt",
    "deadline",
    "explicit",
    "nursery",
    "shutdown",
    "unknown",
]


class TrioInternalError(Exception):
    pass


class RunFinishedError(RuntimeError):
    pass


class WouldBlock(Exception):
    pass


@final
@attrs.define(eq=False, kw_only=True)
class Cancelled(BaseException, metaclass=NoPublicConstructor):

    source: CancelReasonLiteral = "unknown"
    source_task: str | None = None
    reason: str | None = None

    def __str__(self) -> str:
        return (
            f"cancelled due to {self.source}"
            + ("" if self.reason is None else f" with reason {self.reason!r}")
            + ("" if self.source_task is None else f" from task {self.source_task}")
        )

    def __reduce__(self) -> tuple[Callable[[], Cancelled], tuple[()]]:
        return (
            partial(
                Cancelled._create,
                source=self.source,
                source_task=self.source_task,
                reason=self.reason,
            ),
            (),
        )

    if TYPE_CHECKING:
        @classmethod
        def _create(
            cls,
            *,
            source: CancelReasonLiteral = "unknown",
            source_task: str | None = None,
            reason: str | None = None,
        ) -> Self: ...


class BusyResourceError(Exception):
    pass


class ClosedResourceError(Exception):
    pass


class BrokenResourceError(Exception):
    pass


class EndOfChannel(Exception):
    pass
