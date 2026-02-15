"""Integration tests for fetch-repo-metadata.

Real SQLite + real file-based Cache. Only external API calls (PyGithub/httpx) are mocked.
"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from github_data_file_fetcher.db import (
    get_repos_without_metadata,
    init_db,
    insert_files,
    insert_repo_metadata,
)
from github_data_file_fetcher.fetch_repo_metadata import fetch_repo_metadata, fetch_repo_metadata_graphql
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


def _insert_repos(db_path, count=3):
    """Insert files so repos are discoverable."""
    files = [
        {"html_url": f"https://github.com/owner/repo{i}/blob/main/f.md", "sha": f"s{i}"}
        for i in range(count)
    ]
    insert_files(db_path, files)
    return [f"owner/repo{i}" for i in range(count)]


def _make_metadata(**overrides):
    m = {
        "stars": 100, "forks": 20, "watchers": 50, "language": "Python",
        "topics": [], "created_at": None, "updated_at": None,
        "pushed_at": None, "default_branch": "main", "license": None,
        "description": "test",
    }
    m.update(overrides)
    return m


def _mock_repo_obj(**overrides):
    repo = MagicMock()
    repo.stargazers_count = overrides.get("stars", 100)
    repo.forks_count = overrides.get("forks", 20)
    repo.watchers_count = overrides.get("watchers", 50)
    repo.language = overrides.get("language", "Python")
    repo.get_topics.return_value = overrides.get("topics", ["ai", "ml"])
    repo.created_at = None
    repo.updated_at = None
    repo.pushed_at = None
    repo.default_branch = "main"
    repo.license = None
    repo.description = "Test repo"
    return repo


def describe_rest_fetch():

    def it_fetches_metadata_for_repos(db_path):
        _insert_repos(db_path, 2)

        with patch("github_data_file_fetcher.fetch_repo_metadata.fetch_repo_metadata.get_client") as mock:
            client = MagicMock()
            client.cache.get.return_value = None
            client.github.get_repo.return_value = _mock_repo_obj()
            mock.return_value = client

            stats = fetch_repo_metadata(db_path=db_path)

        assert stats["fetched"] == 2
        assert stats["errors"] == 0

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM repo_metadata").fetchone()[0]
        conn.close()
        assert count == 2

    def it_handles_metadata_errors(db_path):
        _insert_repos(db_path, 1)

        with patch("github_data_file_fetcher.fetch_repo_metadata.fetch_repo_metadata.get_client") as mock:
            client = MagicMock()
            client.cache.get.return_value = None
            client.github.get_repo.side_effect = Exception("API Error")
            mock.return_value = client

            stats = fetch_repo_metadata(db_path=db_path)

        assert stats["fetched"] == 0
        assert stats["errors"] == 1


def describe_skip_completed():

    def it_skips_repos_already_in_db(db_path):
        repos = _insert_repos(db_path, 3)
        for rk in repos:
            insert_repo_metadata(db_path, rk, _make_metadata())

        assert len(get_repos_without_metadata(db_path)) == 0

        with patch("github_data_file_fetcher.fetch_repo_metadata.fetch_repo_metadata.get_client") as mock:
            client = MagicMock()
            client.cache = MagicMock()
            mock.return_value = client

            stats = fetch_repo_metadata(db_path=db_path)

        assert stats["fetched"] == 0
        assert stats["errors"] == 0
        client.github.get_repo.assert_not_called()

    def it_skips_repos_already_in_db_graphql(db_path):
        repos = _insert_repos(db_path, 3)
        for rk in repos:
            insert_repo_metadata(db_path, rk, _make_metadata())

        with patch("github_data_file_fetcher.fetch_repo_metadata.fetch_graphql.GraphQLClient") as MockGQL:
            gql = MagicMock()
            MockGQL.return_value = gql

            stats = fetch_repo_metadata_graphql(db_path=db_path)

        assert stats["fetched"] == 0
        gql.fetch_metadata_batch.assert_not_called()


def describe_cache_interaction():

    def it_uses_cache_hit_to_populate_db(db_path, cache_dir):
        """When cache has metadata, REST fetch reads from cache and inserts to DB."""
        repos = _insert_repos(db_path, 1)
        repo_key = repos[0]

        # Pre-populate the file cache with metadata
        cache = Cache(cache_dir)
        cache.set("repo_metadata", {"repo_key": repo_key}, _make_metadata(stars=999))

        with patch("github_data_file_fetcher.fetch_repo_metadata.fetch_repo_metadata.get_client") as mock:
            client = MagicMock()
            client.cache = cache
            mock.return_value = client

            stats = fetch_repo_metadata(db_path=db_path)

        assert stats["fetched"] == 1
        assert stats["cache_hits"] == 1
        # No API call needed
        client.github.get_repo.assert_not_called()

        # Verify DB got the cached data
        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT stars FROM repo_metadata WHERE repo_key = ?", (repo_key,)).fetchone()
        conn.close()
        assert row[0] == 999
