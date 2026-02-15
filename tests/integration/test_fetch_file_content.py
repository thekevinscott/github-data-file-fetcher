"""Integration tests for fetch-file-content.

Real SQLite + real file-based Cache. Only external API calls (PyGithub/httpx) are mocked.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from github import GithubException

from github_data_file_fetcher.db import init_db, insert_files
from github_data_file_fetcher.fetch_file_content import fetch_file_content, fetch_file_content_graphql
from github_data_file_fetcher.github import Cache, GitHubClient


@pytest.fixture
def db_path(tmp_path):
    p = tmp_path / "test.db"
    init_db(p)
    return p


@pytest.fixture
def content_dir(tmp_path):
    d = tmp_path / "content"
    d.mkdir()
    return d


@pytest.fixture
def cache_dir(tmp_path):
    d = tmp_path / "cache"
    d.mkdir()
    return d


@pytest.fixture
def client(cache_dir):
    return GitHubClient(cache_dir=cache_dir)


URLS = [
    "https://github.com/owner/repo/blob/main/path/to/file.md",
    "https://github.com/owner/repo/blob/dev/other/file.md",
]


def _setup_db(db_path, urls=URLS):
    insert_files(db_path, [{"html_url": u, "sha": "abc"} for u in urls])


def _create_content_on_disk(content_dir, urls):
    """Write placeholder files so skip logic triggers."""
    for url in urls:
        rest = url[19:]
        parts = rest.split("/")
        owner, repo, _, ref = parts[0], parts[1], parts[2], parts[3]
        path = "/".join(parts[4:])
        local = content_dir / owner / repo / "blob" / ref / path
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_text("# existing")


def describe_rest_fetch():

    def it_fetches_content_for_urls(db_path, content_dir):
        _setup_db(db_path)

        with patch("github_data_file_fetcher.fetch_file_content.fetch_file_content.get_client") as mock:
            client = MagicMock()
            client.get_file_content.return_value = {"content": "IyBUZXN0IEZpbGU="}
            mock.return_value = client

            stats = fetch_file_content(URLS, content_dir)

        assert stats["fetched"] == 2
        assert stats["errors"] == 0

    def it_handles_fetch_errors(content_dir):
        with patch("github_data_file_fetcher.fetch_file_content.fetch_file_content.get_client") as mock:
            client = MagicMock()
            client.get_file_content.side_effect = Exception("API Error")
            mock.return_value = client

            stats = fetch_file_content(URLS[:1], content_dir)

        assert stats["fetched"] == 0
        assert stats["errors"] == 1

    def it_skips_existing_files_with_zero_api_calls(content_dir):
        _create_content_on_disk(content_dir, URLS)

        with patch("github_data_file_fetcher.fetch_file_content.fetch_file_content.get_client") as mock:
            client = MagicMock()
            mock.return_value = client

            stats = fetch_file_content(URLS, content_dir)

        assert stats["skipped"] == 2
        assert stats["fetched"] == 0
        client.get_file_content.assert_not_called()


def describe_caching():

    def it_caches_404_responses(client):
        mock_repo = MagicMock()
        mock_repo.get_contents.side_effect = GithubException(404, {"message": "Not Found"}, {})
        mock_github = MagicMock()
        mock_github.get_repo.return_value = mock_repo
        client._github = mock_github

        with pytest.raises(FileNotFoundError):
            client.get_file_content("owner", "repo", "missing.md")

        # Verify cached
        cached = client.cache.get(
            "contents", {"owner": "owner", "repo": "repo", "path": "missing.md", "ref": None}
        )
        assert cached is not None
        assert cached["error"] == "not_found"

        # Second call returns cached error, no exception
        result = client.get_file_content("owner", "repo", "missing.md")
        assert result["error"] == "not_found"
        assert mock_github.get_repo.call_count == 1


def describe_graphql_fetch():

    def it_skips_existing_files_on_disk(content_dir):
        _create_content_on_disk(content_dir, URLS)

        with patch("github_data_file_fetcher.fetch_file_content.fetch_graphql.GraphQLClient") as MockGQL:
            gql = MagicMock()
            MockGQL.return_value = gql

            stats = fetch_file_content_graphql(URLS, content_dir)

        assert stats["skipped"] == 2
        assert stats["fetched"] == 0
        gql.fetch_batch.assert_not_called()

    def it_does_not_cache_truncated_blobs(cache_dir):
        from github_data_file_fetcher.graphql import GraphQLClient

        with patch("github_data_file_fetcher.graphql.get_settings") as ms:
            ms.return_value = MagicMock(github_token="fake")
            gql = GraphQLClient(cache_dir=cache_dir)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {"r0": {"f0": {"text": "partial", "byteSize": 200000, "isTruncated": True}}}
        }
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_batch([("owner", "repo", "main", "big.md")])
        assert results[0].error == "truncated"

        cached = gql.cache.get(
            "contents", {"owner": "owner", "repo": "repo", "path": "big.md", "ref": "main"}
        )
        assert cached is None

    def it_skips_cached_items_in_graphql_batch(cache_dir):
        from github_data_file_fetcher.graphql import GraphQLClient

        # Pre-populate cache for one item
        cache = Cache(cache_dir)
        cache.set("contents", {"owner": "o", "repo": "r", "path": "cached.md", "ref": "main"}, {
            "content": "dGVzdA==", "encoding": "base64", "size": 4, "path": "cached.md",
        })

        with patch("github_data_file_fetcher.graphql.get_settings") as ms:
            ms.return_value = MagicMock(github_token="fake")
            gql = GraphQLClient(cache_dir=cache_dir)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {
                "r0": {
                    "f0": {"text": "hello", "byteSize": 5, "isTruncated": False},
                    "f1": {"text": "world", "byteSize": 5, "isTruncated": False},
                }
            }
        }
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        items = [
            ("o", "r", "main", "cached.md"),
            ("o", "r", "main", "new1.md"),
            ("o", "r", "main", "new2.md"),
        ]
        results = gql.fetch_batch(items)

        assert len(results) == 3
        assert results[0].content_b64 == "dGVzdA=="  # from cache

        # GraphQL query should only contain the uncached items
        call_args = gql._client.post.call_args
        query = call_args[1]["json"]["query"]
        assert "cached.md" not in query
        assert "new1.md" in query
        assert "new2.md" in query
