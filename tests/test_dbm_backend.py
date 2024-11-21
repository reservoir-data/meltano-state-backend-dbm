from __future__ import annotations

import shutil
from typing import TYPE_CHECKING

import pytest
from meltano.core.project import Project
from meltano.core.state_store import (
    MeltanoState,
    state_store_manager_from_project_settings,
)

from meltano_dbm_state_backend.backend import DBMStateStoreManager

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def project(tmp_path: Path) -> Project:
    path = tmp_path / "project"
    shutil.copytree(
        "fixtures/project",
        path,
        ignore=shutil.ignore_patterns(".meltano/**"),
    )
    return Project.find(path.resolve())


def test_state_store(tmp_path: Path) -> None:
    path = tmp_path / "test_db"
    manager = DBMStateStoreManager(
        uri=f"dbm://{path.resolve().as_posix()}",
        write_buffer_size=None,
    )

    # Set initial state
    initial_state = MeltanoState(
        state_id="test",
        partial_state={},
        completed_state={"key": "value"},
    )
    manager.set(initial_state)

    # Get state
    state = manager.get("test")
    assert state is not None
    assert state.completed_state == {"key": "value"}
    assert state.partial_state == {}
    assert manager.get_state_ids() == ["test"]

    # Merge partial state with existing state
    new_state = MeltanoState(
        state_id="test",
        partial_state={"key": "value"},
        completed_state={},
    )
    manager.set(new_state)
    state = manager.get("test")
    assert state is not None
    assert state.completed_state == {"key": "value"}
    assert state.partial_state == {"key": "value"}

    # Clear state
    manager.clear("test")
    state = manager.get("test")

    # Set partial state without existing state
    manager.set(new_state)
    state = manager.get("test")
    assert state is not None
    assert state.completed_state == {}
    assert state.partial_state == {"key": "value"}

    # Clear state for good
    manager.clear("test")
    state = manager.get("test")
    assert state is None
    assert manager.get_state_ids() == []


def test_get_manager(project: Project) -> None:
    manager = state_store_manager_from_project_settings(project.settings)

    assert isinstance(manager, DBMStateStoreManager)
    assert manager.scheme == "dbm"
    assert manager.path.endswith(".meltano/state")

    manager.set(
        MeltanoState(
            state_id="test",
            partial_state={},
            completed_state={"key": "value"},
        ),
    )
    sys_dir_root = project.sys_dir_root
    assert list(sys_dir_root.glob("state*"))
