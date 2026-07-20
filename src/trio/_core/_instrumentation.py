from __future__ import annotations

import logging
import types
from collections import UserDict
from typing import TYPE_CHECKING, TypeVar

from .._abc import Instrument

INSTRUMENT_LOGGER = logging.getLogger("trio.abc.Instrument")

if TYPE_CHECKING:
    from collections.abc import Sequence

    T = TypeVar("T")




class Instruments(UserDict[str, dict[Instrument, None]]):

    __slots__ = ()

    def __init__(self, incoming: Sequence[Instrument]) -> None:
        super().__init__({"_all": {}})
        for instrument in incoming:
            self.add_instrument(instrument)

    @_public
    def add_instrument(self, instrument: Instrument) -> None:
        """Start instrumenting the current run loop with the given instrument.

        Args:
          instrument (trio.abc.Instrument): The instrument to activate.

        If ``instrument`` is already active, does nothing.

        """
        if instrument in self.data["_all"]:
            return
        self.data["_all"][instrument] = None
        try:
            for name in dir(instrument):
                if name.startswith("_"):
                    continue
                try:
                    prototype = getattr(Instrument, name)
                except AttributeError:
                    continue
                impl = getattr(instrument, name)
                if isinstance(impl, types.MethodType) and impl.__func__ is prototype:
                    continue
                self.data.setdefault(name, {})[instrument] = None
        except:
            self.remove_instrument(instrument)
            raise

    @_public
    def remove_instrument(self, instrument: Instrument) -> None:
        """Stop instrumenting the current run loop with the given instrument.

        Args:
          instrument (trio.abc.Instrument): The instrument to de-activate.

        Raises:
          KeyError: if the instrument is not currently active. This could
              occur either because you never added it, or because you added it
              and then it raised an unhandled exception and was automatically
              deactivated.

        """
        self.data["_all"].pop(instrument)
        for hookname, instruments in list(self.data.items()):
            if instrument in instruments:
                del instruments[instrument]
                if not instruments:
                    del self.data[hookname]

    def call(
        self,
        hookname: str,
        *args: object,
    ) -> None:
        """Call hookname(*args) on each applicable instrument.

        You must first check whether there are any instruments installed for
        that hook, e.g.::

            if "before_task_step" in instruments:
                instruments.call("before_task_step", task)
        """
        for instrument in list(self.data[hookname]):
            try:
                getattr(instrument, hookname)(*args)
            except BaseException:
                self.remove_instrument(instrument)
                INSTRUMENT_LOGGER.exception(
                    "Exception raised when calling %r on instrument %r. "
                    "Instrument has been disabled.",
                    hookname,
                    instrument,
                )
