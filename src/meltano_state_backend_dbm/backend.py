"""A state store manager that uses shelve as the backend."""

from __future__ import annotations

import dbm
import fnmatch
import json
import typing as t
from contextlib import contextmanager

from meltano.core.state_store.base import MeltanoState, StateStoreManager

if t.TYPE_CHECKING:
    from collections.abc import Generator


def _decode_state_bytes(state: bytes) -> str:
    return state.decode(json.detect_encoding(state), "surrogatepass")


class DBMStateStoreManager(StateStoreManager):
    """A state store manager that uses DBM/shelve as the backend."""

    label: str = "DBM-based shelve state store manager"

    def __init__(
        self,
        uri: str,
        **kwargs: t.Any,
    ) -> None:
        """Create a DBMStateStoreManager.

        Args:
            uri: The URI to use to connect to the shelve database
            **kwargs: Additional keyword arguments.
        """
        super().__init__(**kwargs)
        self.uri = uri
        self.scheme, self.path = uri.split("://", 1)

    def set(self, state: MeltanoState) -> None:
        """Set state for the given state_id.

        Args:
            state: The state to set
        """
        with dbm.open(self.path, "c") as db:
            db[state.state_id] = state.json()

    def get(self, state_id: str) -> MeltanoState | None:
        """Get state for the given state_id.

        Args:
            state_id: The state_id to get state for.

        Returns:
            Dict representing state that would be used in the next run.
        """
        with dbm.open(self.path, "r") as db:
            state: bytes | None = db.get(state_id)

        return (
            MeltanoState.from_json(state_id, _decode_state_bytes(state))
            if state
            else None
        )

    def delete(self, state_id: str) -> None:
        """Clear state for the given state_id.

        Args:
            state_id: the state_id to clear state for
        """
        with dbm.open(self.path, "w") as db:
            del db[state_id]

    def get_state_ids(self, pattern: str | None = None) -> list[str]:
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by

        Returns:
            List of state_ids available in this state store manager.
        """
        with dbm.open(self.path, "r") as db:
            return [
                state_id.decode()  # type: ignore[union-attr]
                # dbm objects are not iterable, so we need to use the keys() method
                # https://github.com/python/cpython/issues/49986
                # https://github.com/python/cpython/issues/53732
                for state_id in db.keys()  # noqa: SIM118
                if not pattern or fnmatch.fnmatch(state_id, pattern)  # type: ignore[type-var]
            ]

    @contextmanager
    def acquire_lock(  # noqa: PLR6301
        self,
        state_id: str,  # noqa: ARG002
        *,
        retry_seconds: int,  # noqa: ARG002
    ) -> Generator[None, None, None]:
        """Acquire a naive lock for the given job's state.

        Args:
            state_id: the state_id to lock
            retry_seconds: the number of seconds to wait before retrying
        """
        yield  # pragma: no cover
