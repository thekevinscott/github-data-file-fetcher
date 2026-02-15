"""E2E tests for fetch-repo-metadata CLI command.

No mocks. Real GitHub API. Uses populated_small_db fixture from conftest.
Skip if GITHUB_TOKEN is not set.
"""

import os
import sqlite3
import subprocess

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("GITHUB_TOKEN"),
    reason="GITHUB_TOKEN required for E2E tests",
)


def _run_cli(*args, output_dir=None, timeout=300):
    cmd = ["uv", "run", "github-fetch"]
    if output_dir:
        cmd.extend(["--output-dir", str(output_dir)])
    cmd.extend(args)
    return subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout,
        cwd="/mnt/work/@work/github-data-file-fetcher",
    )


def test_cold_start(populated_small_db, e2e_output_dir):
    """fetch-repo-metadata populates repo_metadata table."""
    result = _run_cli(
        "fetch-repo-metadata",
        "--db", str(populated_small_db),
        "--graphql", "--batch-size", "20",
        output_dir=e2e_output_dir,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"

    conn = sqlite3.connect(populated_small_db)
    count = conn.execute("SELECT COUNT(*) FROM repo_metadata").fetchone()[0]
    conn.close()
    assert count > 0, "Expected repo metadata in DB"


def test_with_cache(populated_small_db, e2e_output_dir):
    """Second run has 0 repos to fetch."""
    _run_cli(
        "fetch-repo-metadata",
        "--db", str(populated_small_db),
        "--graphql", "--batch-size", "20",
        output_dir=e2e_output_dir,
    )

    result = _run_cli(
        "fetch-repo-metadata",
        "--db", str(populated_small_db),
        "--graphql", "--batch-size", "20",
        output_dir=e2e_output_dir,
    )
    assert result.returncode == 0
    output = result.stdout + result.stderr
    assert "0 need metadata" in output.lower() or "0 fetched" in output.lower(), (
        f"Expected skip indication:\n{output}"
    )
