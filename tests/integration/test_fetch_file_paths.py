"""Integration tests for fetch-file-paths.

Real SQLite + real file-based Cache. Only external API calls (PyGithub) are mocked.
"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from github_data_file_fetcher.db import get_all_urls, get_file_count, init_db, insert_files
from github_data_file_fetcher.fetch_file_paths import fetch_file_paths
from github_data_file_fetcher.github import Cache, GitHubClient


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test.db"
    init_db(p)
    return p


@pytest.fixture
def cache_dir(tmp_path):
    d = tmp_path / "cache"
    d.mkdir()
    return d


@pytest.fixture
def client(cache_dir):
    return GitHubClient(cache_dir=cache_dir)


@pytest.fixture
def mock_github_client():
    """Patch get_client in fetch_file_paths module to return a MagicMock."""
    with patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.get_client") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


def describe_collect_files():

    def it_initializes_database(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "content_status" in tables
        assert "files" in tables
        assert "repo_metadata" in tables
        assert "file_history" in tables
        assert "search_hits" in tables

    def it_collects_files_from_search(db_path, mock_github_client):
        call_count = [0]

        def search_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return {"total_count": 2}
            elif call_count[0] == 2:
                return {"total_count": 2}
            else:
                return {"total_count": 0}

        mock_github_client.search_code.side_effect = search_side_effect

        with patch(
            "github_data_file_fetcher.fetch_file_paths.fetch_file_paths.fetch_files"
        ) as mock_fetch:
            mock_fetch.return_value = [
                {"html_url": "https://github.com/test/repo1/blob/main/CLAUDE.md", "sha": "abc123"},
                {"html_url": "https://github.com/test/repo2/blob/main/CLAUDE.md", "sha": "def456"},
            ]
            fetch_file_paths("filename:CLAUDE.md", db_path=db_path)

        assert get_file_count(db_path) == 2
        urls = get_all_urls(db_path)
        assert any("repo1" in url for url in urls)
        assert any("repo2" in url for url in urls)

    def it_resumes_from_existing_progress(db_path, mock_github_client):
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO files (url, sha) VALUES (?, ?)",
            ("https://github.com/test/repo/blob/main/CLAUDE.md", "abc123"),
        )
        conn.commit()
        conn.close()

        call_count = [0]

        def search_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] <= 2:
                return {"total_count": 1}
            else:
                return {"total_count": 0}

        mock_github_client.search_code.side_effect = search_side_effect

        with patch(
            "github_data_file_fetcher.fetch_file_paths.fetch_file_paths.fetch_files"
        ) as mock_fetch:
            mock_fetch.return_value = [
                {"html_url": "https://github.com/test/repo/blob/main/CLAUDE.md", "sha": "abc123"}
            ]
            fetch_file_paths("filename:CLAUDE.md", db_path=db_path)

        assert get_file_count(db_path) == 1


def describe_search_code_caching():

    def it_caches_result_and_skips_api_on_second_call(client):
        """First call hits API, second call returns cached. API called once."""
        mock_item = MagicMock()
        mock_item.sha = "abc123"
        mock_item.name = "CLAUDE.md"
        mock_item.path = "CLAUDE.md"
        mock_item.html_url = "https://github.com/owner/repo/blob/main/CLAUDE.md"
        mock_item.repository.full_name = "owner/repo"

        mock_results = MagicMock()
        mock_results.totalCount = 1
        mock_results.get_page.return_value = [mock_item]

        mock_github = MagicMock()
        mock_github.search_code.return_value = mock_results
        client._github = mock_github

        result1 = client.search_code("filename:CLAUDE.md")
        assert result1["total_count"] == 1
        assert mock_github.search_code.call_count == 1

        result2 = client.search_code("filename:CLAUDE.md")
        assert result2 == result1
        assert mock_github.search_code.call_count == 1
        assert client.cache.hits == 1

    def it_does_not_cache_empty_later_pages(client):
        """Empty items on page > 1 must NOT be cached (likely transient)."""
        mock_results = MagicMock()
        mock_results.totalCount = 100
        mock_results.get_page.return_value = []

        mock_github = MagicMock()
        mock_github.search_code.return_value = mock_results
        client._github = mock_github

        client.search_code("test query", page=2)
        cached = client.cache.get("search/code", {"q": "test query", "per_page": 100, "page": 2})
        assert cached is None

        # Second call must hit API again
        client.search_code("test query", page=2)
        assert mock_github.search_code.call_count == 2

    def it_caches_empty_page_1(client):
        """Empty items on page 1 IS cached (legitimate empty result)."""
        mock_results = MagicMock()
        mock_results.totalCount = 0
        mock_results.get_page.return_value = []

        mock_github = MagicMock()
        mock_github.search_code.return_value = mock_results
        client._github = mock_github

        client.search_code("empty query", page=1)
        cached = client.cache.get("search/code", {"q": "empty query", "per_page": 100, "page": 1})
        assert cached is not None
        assert cached["items"] == []


def describe_early_exit():

    def it_exits_immediately_when_scan_already_completed(db_path):
        """If scan_progress shows completed, fetch_file_paths returns without any API calls."""
        from github_data_file_fetcher.db import update_scan_progress

        update_scan_progress(db_path, "filename:CLAUDE.md", last_lo=1000000, max_size=1000000, collected=100, completed=True)

        with patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.get_client") as mock_get:
            client = MagicMock()
            mock_get.return_value = client

            fetch_file_paths("filename:CLAUDE.md", db_path=db_path)

        # No API calls should have been made at all
        client.search_code.assert_not_called()

    def it_resumes_from_last_lo_on_interrupted_scan(db_path):
        """If scan was interrupted, resume from the saved lo position."""
        from github_data_file_fetcher.db import update_scan_progress

        # Simulate interrupted scan at lo=500000
        update_scan_progress(db_path, "filename:CLAUDE.md", last_lo=500000, max_size=1000000, collected=50)

        def search_side_effect(query, per_page=100, page=1):
            return {"total_count": 0}

        with patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.get_client") as mock_get:
            client = MagicMock()
            client.search_code.side_effect = search_side_effect
            mock_get.return_value = client

            fetch_file_paths("filename:CLAUDE.md", db_path=db_path)

        # First search_code call should be the total count query
        # Then it should scan from 500000, not from 0
        calls = client.search_code.call_args_list
        assert len(calls) >= 2
        # First call is the total query (no size: param)
        assert "size:" not in calls[0][0][0]
        # Second call should start from 500000
        assert "size:500000.." in calls[1][0][0]
