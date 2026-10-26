from pathlib import Path
from shutil import rmtree
from uuid import uuid4

import pytest


@pytest.fixture
def test_data_dir():
    """Provide isolated app storage under the workspace for filesystem-restricted runs."""
    workspace = Path(__file__).resolve().parents[1]
    root = workspace / ".pytest-runtime-data"
    path = root / uuid4().hex
    path.mkdir(parents=True)
    try:
        yield path
    finally:
        rmtree(path, ignore_errors=True)
        try:
            root.rmdir()
        except OSError:
            pass
