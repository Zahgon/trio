from __future__ import annotations

import inspect
from typing import NoReturn

import pytest

from ..testing import MockClock, trio_test

RUN_SLOW = True
SKIP_OPTIONAL_IMPORTS = False












def skip_if_optional_else_raise(error: ImportError) -> NoReturn:
    if SKIP_OPTIONAL_IMPORTS:
        pytest.skip(error.msg, allow_module_level=True)
    else:  # pragma: no cover
        raise error
