"""Unit tests for db module."""

import json
import sqlite3
from pathlib import Path

import pytest

from .db import (
    get_all_urls,
    get_db,
    get_file_count,
    get_files_without_content,
    get_files_without_history,
    get_multi_range_hits,
    get_repos_without_metadata,
    get_scan_progress,
    update_scan_progress,
    get_unique_repos,
    init_db,
    insert_content_status_batch,
    insert_file_history,
    insert_file_history_batch,
    insert_files,
    insert_repo_metadata,
    insert_repo_metadata_batch,
    insert_search_hits,
)


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test.db"
    init_db(p)
    return p


def _make_files(n, prefix="owner/repo"):
    return [
        {"html_url": f"https://github.com/{prefix}{i}/blob/main/file.md", "sha": f"sha{i:04d}"}
        for i in range(n)
    ]


def _make_metadata():
    return {
        "stars": 100, "forks": 20, "watchers": 50, "language": "Python",
        "topics": ["ai"], "created_at": "2024-01-01T00:00:00", "updated_at": None,
        "pushed_at": None, "default_branch": "main", "license": "MIT",
        "description": "A test repo",
    }


def describe_init_db():

    def it_creates_all_tables(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = sorted(row[0] for row in cursor.fetchall())
        conn.close()
        assert "content_status" in tables
        assert "file_history" in tables
        assert "files" in tables
        assert "repo_metadata" in tables
        assert "search_hits" in tables

    def it_uses_wal_mode(db_path):
        conn = sqlite3.connect(db_path)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.close()
        assert mode == "wal"

    def it_is_idempotent(db_path):
        """Calling init_db twice does not error or destroy data."""
        insert_files(db_path, _make_files(1))
        init_db(db_path)  # second init
        assert get_file_count(db_path) == 1

    def it_creates_parent_directories(tmp_path):
        deep = tmp_path / "a" / "b" / "c" / "test.db"
        init_db(deep)
        assert deep.exists()


def describe_insert_files():

    def it_inserts_new_files(db_path):
        count = insert_files(db_path, _make_files(5))
        assert count == 5

    def it_returns_zero_for_empty_list(db_path):
        count = insert_files(db_path, [])
        assert count == 0

    def it_deduplicates_by_url(db_path):
        files = _make_files(3)
        insert_files(db_path, files)
        count = insert_files(db_path, files)  # same files again
        assert count == 0

    def it_returns_count_of_only_new_files(db_path):
        insert_files(db_path, _make_files(3))
        # Insert 5 files, 3 of which already exist
        mixed = _make_files(5)
        count = insert_files(db_path, mixed)
        assert count == 2  # only 2 new


def describe_get_file_count():

    def it_returns_zero_for_empty_db(db_path):
        assert get_file_count(db_path) == 0

    def it_returns_correct_count(db_path):
        insert_files(db_path, _make_files(7))
        assert get_file_count(db_path) == 7


def describe_get_all_urls():

    def it_returns_empty_for_empty_db(db_path):
        assert get_all_urls(db_path) == []

    def it_returns_all_urls_in_order(db_path):
        files = _make_files(3)
        insert_files(db_path, files)
        urls = get_all_urls(db_path)
        assert len(urls) == 3
        assert urls == [f["html_url"] for f in files]


def describe_get_unique_repos():

    def it_extracts_repos_from_urls(db_path):
        insert_files(db_path, [
            {"html_url": "https://github.com/alice/project1/blob/main/f.md", "sha": "a"},
            {"html_url": "https://github.com/bob/project2/blob/dev/g.md", "sha": "b"},
            {"html_url": "https://github.com/alice/project1/blob/main/h.md", "sha": "c"},
        ])
        repos = get_unique_repos(db_path)
        assert sorted(repos) == ["alice/project1", "bob/project2"]

    def it_returns_empty_for_empty_db(db_path):
        assert get_unique_repos(db_path) == []


def describe_get_repos_without_metadata():

    def it_returns_repos_missing_metadata(db_path):
        insert_files(db_path, [
            {"html_url": "https://github.com/a/r1/blob/main/f.md", "sha": "1"},
            {"html_url": "https://github.com/b/r2/blob/main/f.md", "sha": "2"},
        ])
        insert_repo_metadata(db_path, "a/r1", _make_metadata())

        remaining = get_repos_without_metadata(db_path)
        assert remaining == ["b/r2"]

    def it_returns_empty_when_all_have_metadata(db_path):
        insert_files(db_path, [
            {"html_url": "https://github.com/a/r1/blob/main/f.md", "sha": "1"},
        ])
        insert_repo_metadata(db_path, "a/r1", _make_metadata())
        assert get_repos_without_metadata(db_path) == []


def describe_insert_repo_metadata():

    def it_inserts_metadata(db_path):
        insert_files(db_path, [
            {"html_url": "https://github.com/a/r1/blob/main/f.md", "sha": "1"},
        ])
        insert_repo_metadata(db_path, "a/r1", _make_metadata())

        conn = get_db(db_path)
        row = conn.execute("SELECT * FROM repo_metadata WHERE repo_key = ?", ("a/r1",)).fetchone()
        conn.close()

        assert row["stars"] == 100
        assert row["language"] == "Python"
        assert json.loads(row["topics"]) == ["ai"]

    def it_replaces_on_conflict(db_path):
        insert_repo_metadata(db_path, "a/r1", _make_metadata())
        insert_repo_metadata(db_path, "a/r1", {**_make_metadata(), "stars": 999})

        conn = get_db(db_path)
        row = conn.execute("SELECT stars FROM repo_metadata WHERE repo_key = ?", ("a/r1",)).fetchone()
        conn.close()
        assert row["stars"] == 999


def describe_insert_repo_metadata_batch():

    def it_inserts_multiple(db_path):
        items = [
            ("a/r1", _make_metadata()),
            ("b/r2", {**_make_metadata(), "stars": 50}),
        ]
        insert_repo_metadata_batch(db_path, items)

        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM repo_metadata").fetchone()[0]
        conn.close()
        assert count == 2

    def it_handles_empty_list(db_path):
        insert_repo_metadata_batch(db_path, [])
        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM repo_metadata").fetchone()[0]
        conn.close()
        assert count == 0


def describe_get_files_without_content():

    def it_returns_files_without_content_status(db_path):
        insert_files(db_path, [
            {"html_url": "https://github.com/a/r1/blob/main/f1.md", "sha": "1"},
            {"html_url": "https://github.com/a/r1/blob/main/f2.md", "sha": "2"},
        ])
        insert_content_status_batch(db_path, [
            ("https://github.com/a/r1/blob/main/f1.md", "fetched"),
        ])

        remaining = get_files_without_content(db_path)
        assert remaining == ["https://github.com/a/r1/blob/main/f2.md"]

    def it_returns_empty_when_all_have_content(db_path):
        insert_files(db_path, [
            {"html_url": "https://github.com/a/r1/blob/main/f.md", "sha": "1"},
        ])
        insert_content_status_batch(db_path, [
            ("https://github.com/a/r1/blob/main/f.md", "fetched"),
        ])
        assert get_files_without_content(db_path) == []

    def it_returns_all_when_none_have_content(db_path):
        insert_files(db_path, [
            {"html_url": "https://github.com/a/r1/blob/main/f.md", "sha": "1"},
        ])
        assert len(get_files_without_content(db_path)) == 1


def describe_insert_content_status_batch():

    def it_inserts_multiple(db_path):
        items = [
            ("https://github.com/a/r1/blob/main/f1.md", "fetched"),
            ("https://github.com/a/r1/blob/main/f2.md", "not_found"),
        ]
        insert_content_status_batch(db_path, items)

        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM content_status").fetchone()[0]
        conn.close()
        assert count == 2

    def it_handles_empty_list(db_path):
        insert_content_status_batch(db_path, [])
        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM content_status").fetchone()[0]
        conn.close()
        assert count == 0

    def it_replaces_on_conflict(db_path):
        insert_content_status_batch(db_path, [("url1", "error")])
        insert_content_status_batch(db_path, [("url1", "fetched")])

        conn = get_db(db_path)
        row = conn.execute("SELECT status FROM content_status WHERE url = ?", ("url1",)).fetchone()
        conn.close()
        assert row["status"] == "fetched"


def describe_get_files_without_history():

    def it_returns_files_without_history(db_path):
        insert_files(db_path, [
            {"html_url": "https://github.com/a/r1/blob/main/f1.md", "sha": "1"},
            {"html_url": "https://github.com/a/r1/blob/main/f2.md", "sha": "2"},
        ])
        insert_file_history(db_path, "https://github.com/a/r1/blob/main/f1.md", [])

        remaining = get_files_without_history(db_path)
        assert remaining == ["https://github.com/a/r1/blob/main/f2.md"]

    def it_returns_empty_when_all_have_history(db_path):
        url = "https://github.com/a/r1/blob/main/f.md"
        insert_files(db_path, [{"html_url": url, "sha": "1"}])
        insert_file_history(db_path, url, [])
        assert get_files_without_history(db_path) == []


def describe_insert_file_history():

    def it_inserts_history(db_path):
        url = "https://github.com/a/r1/blob/main/f.md"
        insert_files(db_path, [{"html_url": url, "sha": "1"}])
        commits = [{"sha": "abc", "message": "init"}]
        insert_file_history(db_path, url, commits)

        conn = get_db(db_path)
        row = conn.execute("SELECT commits FROM file_history WHERE url = ?", (url,)).fetchone()
        conn.close()
        assert json.loads(row["commits"]) == commits

    def it_replaces_on_conflict(db_path):
        url = "https://github.com/a/r1/blob/main/f.md"
        insert_files(db_path, [{"html_url": url, "sha": "1"}])
        insert_file_history(db_path, url, [{"sha": "old"}])
        insert_file_history(db_path, url, [{"sha": "new"}])

        conn = get_db(db_path)
        row = conn.execute("SELECT commits FROM file_history WHERE url = ?", (url,)).fetchone()
        conn.close()
        assert json.loads(row["commits"]) == [{"sha": "new"}]


def describe_insert_file_history_batch():

    def it_inserts_multiple(db_path):
        urls = [
            "https://github.com/a/r1/blob/main/f1.md",
            "https://github.com/a/r1/blob/main/f2.md",
        ]
        for url in urls:
            insert_files(db_path, [{"html_url": url, "sha": "x"}])

        items = [(url, [{"sha": "abc"}]) for url in urls]
        insert_file_history_batch(db_path, items)

        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM file_history").fetchone()[0]
        conn.close()
        assert count == 2

    def it_handles_empty_list(db_path):
        insert_file_history_batch(db_path, [])
        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM file_history").fetchone()[0]
        conn.close()
        assert count == 0


def describe_insert_search_hits():

    def it_inserts_hits(db_path):
        hits = [
            {"url": "https://github.com/a/r1/blob/main/f.md", "query": "q1", "size_min": 0, "size_max": 100},
            {"url": "https://github.com/a/r1/blob/main/f.md", "query": "q2", "size_min": 100, "size_max": 200},
        ]
        insert_search_hits(db_path, hits)

        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM search_hits").fetchone()[0]
        conn.close()
        assert count == 2

    def it_does_not_deduplicate(db_path):
        """search_hits appends, does not deduplicate."""
        hit = {"url": "https://github.com/a/r1/blob/main/f.md", "query": "q", "size_min": 0, "size_max": 100}
        insert_search_hits(db_path, [hit])
        insert_search_hits(db_path, [hit])

        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM search_hits").fetchone()[0]
        conn.close()
        assert count == 2

    def it_handles_empty_list(db_path):
        insert_search_hits(db_path, [])
        conn = get_db(db_path)
        count = conn.execute("SELECT COUNT(*) FROM search_hits").fetchone()[0]
        conn.close()
        assert count == 0


def describe_get_multi_range_hits():

    def it_finds_urls_in_multiple_ranges(db_path):
        url = "https://github.com/a/r1/blob/main/f.md"
        hits = [
            {"url": url, "query": "q", "size_min": 0, "size_max": 100},
            {"url": url, "query": "q", "size_min": 100, "size_max": 200},
        ]
        insert_search_hits(db_path, hits)

        results = get_multi_range_hits(db_path)
        assert len(results) == 1
        assert results[0]["url"] == url
        assert results[0]["range_count"] == 2

    def it_returns_empty_when_no_multi_range(db_path):
        hits = [
            {"url": "https://github.com/a/r1/blob/main/f.md", "query": "q", "size_min": 0, "size_max": 100},
        ]
        insert_search_hits(db_path, hits)

        results = get_multi_range_hits(db_path)
        assert results == []


def describe_scan_progress():

    def it_returns_none_for_unknown_query(db_path):
        assert get_scan_progress(db_path, "unknown") is None

    def it_saves_and_retrieves_progress(db_path):
        update_scan_progress(db_path, "test query", last_lo=5000, max_size=1000000, collected=42)
        p = get_scan_progress(db_path, "test query")
        assert p is not None
        assert p["last_lo"] == 5000
        assert p["max_size"] == 1000000
        assert p["collected"] == 42
        assert p["completed_at"] is None

    def it_marks_completed(db_path):
        update_scan_progress(db_path, "q", last_lo=1000000, max_size=1000000, collected=100, completed=True)
        p = get_scan_progress(db_path, "q")
        assert p["completed_at"] is not None

    def it_updates_existing_progress(db_path):
        update_scan_progress(db_path, "q", last_lo=100, max_size=1000000, collected=5)
        update_scan_progress(db_path, "q", last_lo=200, max_size=1000000, collected=10)
        p = get_scan_progress(db_path, "q")
        assert p["last_lo"] == 200
        assert p["collected"] == 10
