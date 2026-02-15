"""Integration tests for fetch-file-history.

Real SQLite + real file-based Cache. Only external API calls (PyGithub/httpx) are mocked.
"""

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from github_data_file_fetcher.db import (
    get_files_without_history,
    init_db,
    insert_file_history,
    insert_files,
)
from github_data_file_fetcher.fetch_file_history import fetch_file_history, fetch_file_history_graphql
from github_data_file_fetcher.github import Cache


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


def _insert_test_files(db_path, count=3):
    urls = [
        f"https://github.com/owner/repo{i}/blob/main/file.md"
        for i in range(count)
    ]
    insert_files(db_path, [{"html_url": u, "sha": f"s{i}"} for i, u in enumerate(urls)])
    return urls


def _mock_commit(sha="abc123def", author="Author", message="commit msg"):
    c = MagicMock()
    c.sha = sha
    c.commit.author.name = author
    c.commit.author.date = None
    c.commit.message = message
    return c


def describe_rest_fetch():

    def it_fetches_commit_history(db_path):
        url = "https://github.com/owner/repo/blob/main/path/file.md"
        insert_files(db_path, [{"html_url": url, "sha": "abc"}])

        with patch("github_data_file_fetcher.fetch_file_history.fetch_file_history.get_client") as mock:
            client = MagicMock()
            client.cache.get.return_value = None
            repo_mock = MagicMock()
            repo_mock.get_commits.return_value = [_mock_commit()]
            client.github.get_repo.return_value = repo_mock
            mock.return_value = client

            stats = fetch_file_history(db_path=db_path)

        assert stats["fetched"] == 1
        assert stats["errors"] == 0

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT commits FROM file_history WHERE url = ?", (url,)).fetchone()
        conn.close()
        assert row is not None
        commits = json.loads(row[0])
        assert len(commits) == 1
        assert commits[0]["sha"] == "abc123d"

    def it_handles_history_errors(db_path):
        url = "https://github.com/owner/repo/blob/main/file.md"
        insert_files(db_path, [{"html_url": url, "sha": "abc"}])

        with patch("github_data_file_fetcher.fetch_file_history.fetch_file_history.get_client") as mock:
            client = MagicMock()
            client.cache.get.return_value = None
            client.github.get_repo.side_effect = Exception("API Error")
            mock.return_value = client

            stats = fetch_file_history(db_path=db_path)

        assert stats["fetched"] == 0
        assert stats["errors"] == 1


def describe_skip_completed():

    def it_skips_files_already_with_history(db_path):
        urls = _insert_test_files(db_path, 3)
        for url in urls:
            insert_file_history(db_path, url, [{"sha": "abc", "message": "init"}])

        assert len(get_files_without_history(db_path)) == 0

        with patch("github_data_file_fetcher.fetch_file_history.fetch_file_history.get_client") as mock:
            client = MagicMock()
            client.cache = MagicMock()
            mock.return_value = client

            stats = fetch_file_history(db_path=db_path)

        assert stats["fetched"] == 0
        assert stats["errors"] == 0
        client.github.get_repo.assert_not_called()

    def it_skips_files_already_with_history_graphql(db_path):
        urls = _insert_test_files(db_path, 3)
        for url in urls:
            insert_file_history(db_path, url, [{"sha": "abc", "message": "init"}])

        with patch("github_data_file_fetcher.fetch_file_history.fetch_graphql.GraphQLClient") as MockGQL:
            gql = MagicMock()
            MockGQL.return_value = gql

            stats = fetch_file_history_graphql(db_path=db_path)

        assert stats["fetched"] == 0
        gql.fetch_history_batch.assert_not_called()


def describe_cache_interaction():

    def it_uses_cache_hit_to_populate_db(db_path, cache_dir):
        """When cache has history, REST fetch reads from cache and inserts to DB."""
        url = "https://github.com/owner/repo/blob/main/file.md"
        insert_files(db_path, [{"html_url": url, "sha": "abc"}])

        cache = Cache(cache_dir)
        cache.set("file_history", {"owner": "owner", "repo": "repo", "path": "file.md"}, {
            "commits": [{"sha": "cached1", "message": "from cache"}],
        })

        with patch("github_data_file_fetcher.fetch_file_history.fetch_file_history.get_client") as mock:
            client = MagicMock()
            client.cache = cache
            mock.return_value = client

            stats = fetch_file_history(db_path=db_path)

        assert stats["fetched"] == 1
        assert stats["cache_hits"] == 1
        client.github.get_repo.assert_not_called()

        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT commits FROM file_history WHERE url = ?", (url,)).fetchone()
        conn.close()
        commits = json.loads(row[0])
        assert commits[0]["sha"] == "cached1"
