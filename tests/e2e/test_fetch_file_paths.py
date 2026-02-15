"""E2E tests for fetch-file-paths CLI command.

No mocks. Real GitHub API, real SQLite, real file cache.
Skip if GITHUB_TOKEN is not set.

Tests two query sizes (<100 files, <1000 files) each cold and with cache.
"""

import os
import sqlite3
import subprocess

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("GITHUB_TOKEN"),
    reason="GITHUB_TOKEN required for E2E tests",
)

SMALL_QUERY = "filename:CLAUDE.md repo:anthropics/courses"  # <100 files
MEDIUM_QUERY = "filename:CLAUDE.md size:0..200"  # <1000 files


def _run_cli(*args, output_dir=None, timeout=300):
    cmd = ["uv", "run", "github-fetch"]
    if output_dir:
        cmd.extend(["--output-dir", str(output_dir)])
    cmd.extend(args)
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout,
        cwd="/mnt/work/@work/github-data-file-fetcher",
    )


def test_small_cold_start(e2e_output_dir):
    """Cold start with <100 files populates the DB."""
    db_path = e2e_output_dir / "files.db"
    result = _run_cli(
        "fetch-file-paths", SMALL_QUERY,
        "--db", str(db_path),
        output_dir=e2e_output_dir,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    conn.close()
    assert 0 < count < 100, f"Expected <100 files, got {count}"


def test_small_with_cache(e2e_output_dir):
    """Second run with <100 files uses cache and exits instantly."""
    db_path = e2e_output_dir / "files.db"

    # First run
    r1 = _run_cli("fetch-file-paths", SMALL_QUERY, "--db", str(db_path), output_dir=e2e_output_dir)
    assert r1.returncode == 0

    # Second run -- should hit scan_progress early exit
    r2 = _run_cli("fetch-file-paths", SMALL_QUERY, "--db", str(db_path), output_dir=e2e_output_dir)
    assert r2.returncode == 0
    output = r2.stdout + r2.stderr
    assert "already completed" in output.lower(), f"Expected early exit:\n{output}"


def test_medium_cold_start(e2e_output_dir):
    """Cold start with <1000 files populates the DB."""
    db_path = e2e_output_dir / "files.db"
    result = _run_cli(
        "fetch-file-paths", MEDIUM_QUERY,
        "--db", str(db_path),
        output_dir=e2e_output_dir,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    conn.close()
    assert 0 < count < 1000, f"Expected <1000 files, got {count}"


def test_medium_with_cache(e2e_output_dir):
    """Second run with <1000 files uses cache and exits instantly."""
    db_path = e2e_output_dir / "files.db"

    r1 = _run_cli("fetch-file-paths", MEDIUM_QUERY, "--db", str(db_path), output_dir=e2e_output_dir)
    assert r1.returncode == 0

    r2 = _run_cli("fetch-file-paths", MEDIUM_QUERY, "--db", str(db_path), output_dir=e2e_output_dir)
    assert r2.returncode == 0
    output = r2.stdout + r2.stderr
    assert "already completed" in output.lower(), f"Expected early exit:\n{output}"
