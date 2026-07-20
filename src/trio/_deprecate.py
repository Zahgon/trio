from __future__ import annotations

import sys
import warnings
from functools import wraps
from typing import TYPE_CHECKING, ClassVar, TypeVar

import attrs

from ._util import final

if TYPE_CHECKING:
    from collections.abc import Callable

    from typing_extensions import ParamSpec

    ArgsT = ParamSpec("ArgsT")

RetT = TypeVar("RetT")


class TrioDeprecationWarning(FutureWarning):
    pass


def _url_for_issue(issue: int) -> str:
    return f"https://github.com/python-trio/trio/issues/{issue}"


def _stringify(thing: object) -> str:
    if hasattr(thing, "__module__") and hasattr(thing, "__qualname__"):
        return f"{thing.__module__}.{thing.__qualname__}"
    return str(thing)


def warn_deprecated(
    thing: object,
    version: str,
    *,
    issue: int | None,
    instead: object,
    stacklevel: int = 2,
    use_triodeprecationwarning: bool = False,
) -> None:
    stacklevel += 1
    msg = f"{_stringify(thing)} is deprecated since Trio {version}"
    if instead is None:
        msg += " with no replacement"
    else:
        msg += f"; use {_stringify(instead)} instead"
    if issue is not None:
        msg += f" ({_url_for_issue(issue)})"
    if use_triodeprecationwarning:
        warning_class: type[Warning] = TrioDeprecationWarning
    else:
        warning_class = DeprecationWarning
    warnings.warn(warning_class(msg), stacklevel=stacklevel)


def deprecated(
    version: str,
    *,
    thing: object = None,
    issue: int | None,
    instead: object,
    use_triodeprecationwarning: bool = False,
) -> Callable[[Callable[ArgsT, RetT]], Callable[ArgsT, RetT]]:

    return do_wrap


def deprecated_alias(
    old_qualname: str,
    new_fn: Callable[ArgsT, RetT],
    version: str,
    *,
    issue: int | None,
) -> Callable[ArgsT, RetT]:
    @deprecated(version, issue=issue, instead=new_fn)
    @wraps(new_fn, assigned=("__module__", "__annotations__"))
    def wrapper(*args: ArgsT.args, **kwargs: ArgsT.kwargs) -> RetT:
        pass

    wrapper.__qualname__ = old_qualname
    wrapper.__name__ = old_qualname.rpartition(".")[-1]
    return wrapper


@final
@attrs.frozen(slots=False)
class DeprecatedAttribute:
    _not_set: ClassVar[object] = object()

    value: object
    version: str
    issue: int | None
    instead: object = _not_set


def deprecate_attributes(
    module_name: str, deprecated_attributes: dict[str, DeprecatedAttribute]
) -> None:
    def __getattr__(name: str) -> object:
        if name in deprecated_attributes:
            info = deprecated_attributes[name]
            instead = info.instead
            if instead is DeprecatedAttribute._not_set:
                instead = info.value
            thing = f"{module_name}.{name}"
            warn_deprecated(thing, info.version, issue=info.issue, instead=instead)
            return info.value

        msg = "module '{}' has no attribute '{}'"
        raise AttributeError(msg.format(module_name, name))

    sys.modules[module_name].__getattr__ = __getattr__  # type: ignore[method-assign]
