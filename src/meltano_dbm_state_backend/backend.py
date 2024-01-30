"""A state store manager that uses shelve as the backend."""

from __future__ import annotations

import fnmatch
import shelve
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
            with shelve.open(self.path, "c") as db:  # noqa: S301
                db[state.state_id] = {
                    "completed": state.completed_state,
                    "partial": state.partial_state,
                }
                return

        with shelve.open(self.path, "c") as db:  # noqa: S301
            existing_state: dict[str, t.Any] | None = db.get(state.state_id)

            if existing_state:
                state_to_write = JobState(
                    state_id=state.state_id,
                    completed_state=existing_state.get("completed", {}),
                    partial_state=existing_state.get("partial", {}),
                )
                state_to_write.merge_partial(state)
            else:
                state_to_write = state

            db[state.state_id] = {
                "completed": state_to_write.completed_state,
                "partial": state_to_write.partial_state,
            }

    def get(self, state_id: str) -> JobState | None:
        """Get state for the given state_id.
        Args:
            state_id: The state_id to get state for
        Returns:
            Dict representing state that would be used in the next run.
        """
        with shelve.open(self.path, "r") as db:  # noqa: S301
            state_dict: dict[str, t.Any] | None = db.get(state_id)

        return (
            JobState(
                state_id=state_id,
                completed_state=state_dict.get("completed", {}),
                partial_state=state_dict.get("partial", {}),
            )
            if state_dict
            else None
        )

    def clear(self, state_id: str) -> None:
        """Clear state for the given state_id.
        Args:
            state_id: the state_id to clear state for
        """
        with shelve.open(self.path, "w") as db:  # noqa: S301
            del db[state_id]

    def get_state_ids(self, pattern: str | None = None) -> list[str]:
        """Get all state_ids available in this state store manager.
        Args:
            pattern: glob-style pattern to filter by
        Returns:
            List of state_ids available in this state store manager.
        """
        with shelve.open(self.path, "r") as db:  # noqa: S301
            return [
                state_id
                for state_id in db
                if not pattern or fnmatch.fnmatch(state_id, pattern)
            ]

    def acquire_lock(self, state_id: str) -> None:
        """Acquire a naive lock for the given job's state.
        Args:
            state_id: the state_id to lock
        """
        pass  # pragma: no cover
