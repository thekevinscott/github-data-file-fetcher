"""E2E test fixtures: real API, isolated temp directories."""

import os
import subprocess

import pytest

from github_data_file_fetcher.db import init_db


def _run_cli(*args, output_dir=None, timeout=300):
    """Run the github-fetch CLI and return CompletedProcess."""
    cmd = ["uv", "run", "github-fetch"]
    if output_dir:
        cmd.extend(["--output-dir", str(output_dir)])
    cmd.extend(args)
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout,
        cwd="/mnt/work/@work/github-data-file-fetcher",
    )


@pytest.fixture
def e2e_db(tmp_path):
    """Isolated temp SQLite DB for E2E tests."""
    db_path = tmp_path / "e2e.db"
    init_db(db_path)
    return db_path


@pytest.fixture
def e2e_cache(tmp_path):
    """Isolated temp cache directory (NOT ~/.cache, to avoid polluting real cache)."""
    d = tmp_path / "cache"
    d.mkdir()
    return d


@pytest.fixture
def e2e_content(tmp_path):
    """Isolated temp content directory."""
    d = tmp_path / "content"
    d.mkdir()
    return d


@pytest.fixture
def e2e_output_dir(tmp_path):
    """Isolated output dir for CLI invocations."""
    d = tmp_path / "results"
    d.mkdir()
    return d


@pytest.fixture
def populated_small_db(e2e_output_dir):
    """DB populated with <100 files from a small query. Shared fixture for content/metadata/history tests."""
    db_path = e2e_output_dir / "files.db"
    result = _run_cli(
        "fetch-file-paths", "filename:CLAUDE.md repo:anthropics/courses",
        "--db", str(db_path),
        output_dir=e2e_output_dir,
    )
    assert result.returncode == 0, f"fetch-file-paths failed:\n{result.stdout}\n{result.stderr}"
    return db_path
