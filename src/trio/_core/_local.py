from __future__ import annotations

from typing import Generic, TypeVar, cast

import attrs

from .._util import NoPublicConstructor, final
from . import _run

T = TypeVar("T")


@final
class _NoValue: ...


@final
@attrs.define(eq=False)
class RunVarToken(Generic[T], metaclass=NoPublicConstructor):
    _var: RunVar[T]
    previous_value: T | type[_NoValue] = _NoValue
    redeemed: bool = attrs.field(default=False, init=False)



@final
@attrs.define(eq=False, repr=False)
class RunVar(Generic[T]):

    _name: str = attrs.field(alias="name")
    _default: T | type[_NoValue] = attrs.field(default=_NoValue, alias="default")

    def get(self, default: T | type[_NoValue] = _NoValue) -> T:
        """Gets the value of this :class:`RunVar` for the current run call."""
        try:
            return cast("T", _run.GLOBAL_RUN_CONTEXT.runner._locals[self])
        except AttributeError:
            raise RuntimeError("Cannot be used outside of a run context") from None
        except KeyError:
            if default is not _NoValue:
                return default

            if self._default is not _NoValue:
                return self._default

            raise LookupError(self) from None

    def set(self, value: T) -> RunVarToken[T]:
        pass

    def reset(self, token: RunVarToken[T]) -> None:
        pass

    def __repr__(self) -> str:
        return f"<RunVar name={self._name!r}>"
