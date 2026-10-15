"""Structured errors raised by the Rust runtime bridge."""

from __future__ import annotations


class NextralError(Exception):
    """A runtime failure with the original, stable core error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message}


class InvalidInputError(NextralError):
    """The runtime rejected an input or configuration."""


class NotFoundError(NextralError):
    """The scoped resource does not exist."""


class ConflictError(NextralError):
    """The requested operation conflicts with existing state."""


class StorageError(NextralError):
    """An underlying I/O operation failed."""


class SerializationError(NextralError):
    """The runtime could not serialize or deserialize a value."""


_ERROR_TYPES: dict[str, type[NextralError]] = {
    "invalid_input": InvalidInputError,
    "not_found": NotFoundError,
    "conflict": ConflictError,
    "io": StorageError,
    "serialization": SerializationError,
}


def from_core_error(code: str, message: str) -> NextralError:
    return _ERROR_TYPES.get(code, NextralError)(code, message)
