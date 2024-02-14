"""A state store manager that uses shelve as the backend."""

from __future__ import annotations

import dbm
import fnmatch
import typing as t

from meltano.core.job_state import JobState
from meltano.core.state_store.base import StateStoreManager


class DBMStateStoreManager(StateStoreManager):  # type: ignore[misc]
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

    def set(self, state: JobState) -> None:
        """Set state for the given state_id.

        Args:
            state: The state to set
        """
        if state.is_complete():
            with dbm.open(self.path, "c") as db:  # noqa: S301
                db[state.state_id] = state.json()
                return

        with dbm.open(self.path, "c") as db:  # noqa: S301
            if existing_state := db.get(state.state_id):
                state_to_write = JobState.from_json(state.state_id, existing_state)
                state_to_write.merge_partial(state)
            else:
                state_to_write = state

            db[state.state_id] = state_to_write.json()

    def get(self, state_id: str) -> JobState | None:
        """Get state for the given state_id.
        Args:
            state_id: The state_id to get state for
        Returns:
            Dict representing state that would be used in the next run.
        """
        with dbm.open(self.path, "r") as db:  # noqa: S301
            state: bytes | None = db.get(state_id)

        return JobState.from_json(state_id, state) if state else None

    def clear(self, state_id: str) -> None:
        """Clear state for the given state_id.
        Args:
            state_id: the state_id to clear state for
        """
        with dbm.open(self.path, "w") as db:  # noqa: S301
            del db[state_id]

    def get_state_ids(self, pattern: str | None = None) -> list[str]:
        """Get all state_ids available in this state store manager.
        Args:
            pattern: glob-style pattern to filter by
        Returns:
            List of state_ids available in this state store manager.
        """
        with dbm.open(self.path, "r") as db:  # noqa: S301
            return [
                state_id.decode()  # type: ignore[union-attr]
                # dbm objects are not iterable, so we need to use the keys() method
                # https://github.com/python/cpython/issues/49986
                # https://github.com/python/cpython/issues/53732
                for state_id in db.keys()  # noqa: SIM118
                if not pattern or fnmatch.fnmatch(state_id, pattern)  # type: ignore[type-var]
            ]

    def acquire_lock(self, state_id: str) -> None:
        """Acquire a naive lock for the given job's state.
        Args:
            state_id: the state_id to lock
        """
        pass  # pragma: no cover
