"""E2E tests for fetch-file-content CLI command.

No mocks. Real GitHub API. Uses populated_small_db fixture from conftest.
Skip if GITHUB_TOKEN is not set.
"""

import os
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
    """fetch-file-content downloads files to disk."""
    content_dir = e2e_output_dir / "content"
    result = _run_cli(
        "fetch-file-content",
        "--db", str(populated_small_db),
        "--content-dir", str(content_dir),
        "--graphql", "--batch-size", "20",
        output_dir=e2e_output_dir,
    )
    assert result.returncode == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"
    content_files = list(content_dir.rglob("*.md"))
    assert len(content_files) > 0, "Expected content files on disk"


def test_with_cache(populated_small_db, e2e_output_dir):
    """Second run skips all files already on disk."""
    content_dir = e2e_output_dir / "content"

    # First fetch
    _run_cli(
        "fetch-file-content",
        "--db", str(populated_small_db),
        "--content-dir", str(content_dir),
        "--graphql", "--batch-size", "20",
        output_dir=e2e_output_dir,
    )

    # Second fetch -- should skip all
    result = _run_cli(
        "fetch-file-content",
        "--db", str(populated_small_db),
        "--content-dir", str(content_dir),
        "--graphql", "--batch-size", "20",
        output_dir=e2e_output_dir,
    )
    assert result.returncode == 0
    output = result.stdout + result.stderr
    assert "skip" in output.lower() or "0 fetched" in output.lower(), (
        f"Expected skip indication:\n{output}"
    )
