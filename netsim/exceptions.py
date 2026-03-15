"""NetSim-specific exceptions."""

from __future__ import annotations

from typing import Any


class EmptySchedule(Exception):
    """Raised when there are no further events to process."""


class Interrupt(Exception):
    """Thrown into a process when it is interrupted.

    The ``cause`` attribute provides the reason for the interrupt, if any.
    """

    def __init__(self, cause: Any = None) -> None:
        super().__init__(cause)

    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self.cause!r})'

    @property
    def cause(self) -> Any:
        """The cause of the interrupt, or ``None``."""
        return self.args[0]
