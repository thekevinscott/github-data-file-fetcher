"""Unit tests for graphql module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from .graphql import (
    GraphQLClient,
    _build_history_query,
    _build_metadata_query,
    _build_query,
    _escape_graphql_string,
    _make_alias,
    _parse_retry_after,
)


def describe_escape_graphql_string():

    def it_escapes_backslashes():
        assert _escape_graphql_string("a\\b") == "a\\\\b"

    def it_escapes_quotes():
        assert _escape_graphql_string('a"b') == 'a\\"b'

    def it_handles_combined():
        assert _escape_graphql_string('a\\"b') == 'a\\\\\\"b'

    def it_handles_empty():
        assert _escape_graphql_string("") == ""

    def it_passes_through_normal_strings():
        assert _escape_graphql_string("main:src/file.py") == "main:src/file.py"


def describe_make_alias():

    def it_returns_r_prefix():
        assert _make_alias(0) == "r0"
        assert _make_alias(5) == "r5"
        assert _make_alias(99) == "r99"


def describe_build_query():

    def it_builds_single_item():
        items = [("owner", "repo", "main", "file.py")]
        query = _build_query(items)
        assert "repository" in query
        assert "owner" in query
        assert "repo" in query
        assert "main:file.py" in query
        assert "Blob" in query

    def it_groups_by_repo():
        items = [
            ("owner", "repo", "main", "a.py"),
            ("owner", "repo", "main", "b.py"),
        ]
        query = _build_query(items)
        # Should have one repository block with two file aliases
        assert query.count("repository(") == 1
        assert "f0:" in query
        assert "f1:" in query

    def it_separates_different_repos():
        items = [
            ("alice", "r1", "main", "a.py"),
            ("bob", "r2", "main", "b.py"),
        ]
        query = _build_query(items)
        assert query.count("repository(") == 2

    def it_escapes_special_characters_in_paths():
        items = [("o", "r", "main", 'path/with"quote.py')]
        query = _build_query(items)
        assert '\\"' in query


def describe_build_metadata_query():

    def it_builds_for_single_repo():
        query = _build_metadata_query(["owner/repo"])
        assert "stargazerCount" in query
        assert "forkCount" in query
        assert "owner" in query
        assert "repo" in query

    def it_builds_for_multiple_repos():
        query = _build_metadata_query(["a/r1", "b/r2", "c/r3"])
        assert query.count("repository(") == 3
        assert "r0:" in query
        assert "r1:" in query
        assert "r2:" in query


def describe_build_history_query():

    def it_builds_single_item():
        items = [("owner", "repo", "main", "file.py")]
        query = _build_history_query(items)
        assert "repository(" in query
        assert "history(" in query
        assert "oid" in query

    def it_groups_by_repo_and_ref():
        items = [
            ("owner", "repo", "main", "a.py"),
            ("owner", "repo", "main", "b.py"),
            ("owner", "repo", "dev", "c.py"),
        ]
        query = _build_history_query(items)
        # One repo, two ref blocks
        assert query.count("repository(") == 1
        assert "ref0:" in query
        assert "ref1:" in query


def describe_parse_retry_after():

    def it_parses_numeric_header():
        resp = MagicMock()
        resp.headers = {"retry-after": "30"}
        assert _parse_retry_after(resp) == 30.0

    def it_parses_float_header():
        resp = MagicMock()
        resp.headers = {"retry-after": "1.5"}
        assert _parse_retry_after(resp) == 1.5

    def it_returns_none_for_missing():
        resp = MagicMock()
        resp.headers = {}
        assert _parse_retry_after(resp) is None

    def it_returns_none_for_invalid():
        resp = MagicMock()
        resp.headers = {"retry-after": "not-a-number"}
        assert _parse_retry_after(resp) is None


def describe_GraphQLClient():

    @pytest.fixture
    def gql(tmp_path):
        with patch("github_data_file_fetcher.graphql.get_settings") as ms:
            ms.return_value = MagicMock(github_token="fake-token")
            client = GraphQLClient(cache_dir=tmp_path / "cache")
        return client

    def it_tracks_query_stats(gql):
        assert gql.queries == 0
        assert gql.rate_limit_hits == 0
        assert gql.avg_query_time == 0

    def it_returns_all_cached_items_without_api_call(gql):
        # Pre-populate cache
        gql.cache.set("contents", {"owner": "o", "repo": "r", "path": "f.py", "ref": "m"}, {
            "content": "abc", "encoding": "base64", "size": 3, "path": "f.py",
        })

        gql._client = MagicMock()
        results = gql.fetch_batch([("o", "r", "m", "f.py")])

        assert len(results) == 1
        assert results[0].content_b64 == "abc"
        gql._client.post.assert_not_called()

    def it_returns_all_cached_metadata_without_api_call(gql):
        gql.cache.set("repo_metadata", {"repo_key": "a/b"}, {"stars": 10})

        gql._client = MagicMock()
        results = gql.fetch_metadata_batch(["a/b"])

        assert len(results) == 1
        assert results[0].metadata == {"stars": 10}
        gql._client.post.assert_not_called()

    def it_returns_all_cached_history_without_api_call(gql):
        gql.cache.set("file_history", {"owner": "o", "repo": "r", "path": "f.py"}, {
            "commits": [{"sha": "abc"}],
        })

        gql._client = MagicMock()
        results = gql.fetch_history_batch([("o", "r", "main", "f.py")])

        assert len(results) == 1
        assert results[0].commits == [{"sha": "abc"}]
        gql._client.post.assert_not_called()

    def it_executes_query_and_maps_results(gql):
        """fetch_batch: uncached items hit API and get mapped correctly."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {
                "r0": {
                    "f0": {"text": "hello world", "byteSize": 11, "isTruncated": False},
                }
            }
        }
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_batch([("owner", "repo", "main", "file.py")])
        assert len(results) == 1
        assert results[0].content_b64 is not None
        assert results[0].error is None
        # Should be cached now
        cached = gql.cache.get("contents", {"owner": "owner", "repo": "repo", "path": "file.py", "ref": "main"})
        assert cached is not None

    def it_handles_not_found_repos(gql):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"r0": None}}
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_batch([("owner", "gone", "main", "f.py")])
        assert results[0].error == "not_found"

    def it_handles_not_found_files(gql):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"r0": {"f0": None}}}
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_batch([("o", "r", "main", "missing.py")])
        assert results[0].error == "not_found"

    def it_handles_binary_files(gql):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {"r0": {"f0": {"text": None, "byteSize": 500, "isTruncated": False}}}
        }
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_batch([("o", "r", "main", "img.png")])
        assert results[0].error == "no_content"

    def it_fetches_metadata_from_api(gql):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {
                "r0": {
                    "stargazerCount": 100,
                    "forkCount": 20,
                    "watchers": {"totalCount": 50},
                    "primaryLanguage": {"name": "Python"},
                    "repositoryTopics": {"nodes": [{"topic": {"name": "ai"}}]},
                    "createdAt": "2024-01-01",
                    "updatedAt": "2024-06-01",
                    "pushedAt": "2024-06-15",
                    "defaultBranchRef": {"name": "main"},
                    "licenseInfo": {"spdxId": "MIT"},
                    "description": "A repo",
                }
            }
        }
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_metadata_batch(["owner/repo"])
        assert len(results) == 1
        assert results[0].metadata["stars"] == 100
        assert results[0].metadata["language"] == "Python"
        assert results[0].metadata["topics"] == ["ai"]

    def it_fetches_history_from_api(gql):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "data": {
                "r0": {
                    "ref0": {
                        "f0": {
                            "nodes": [
                                {
                                    "oid": "abc1234567890",
                                    "messageHeadline": "init commit",
                                    "committedDate": "2024-01-01T00:00:00Z",
                                    "author": {"name": "Dev"},
                                }
                            ]
                        }
                    }
                }
            }
        }
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_history_batch([("owner", "repo", "main", "file.py")])
        assert len(results) == 1
        assert results[0].commits is not None
        assert len(results[0].commits) == 1
        assert results[0].commits[0]["sha"] == "abc1234"
        assert results[0].commits[0]["author"] == "Dev"

    def it_handles_metadata_not_found(gql):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"r0": None}}
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_metadata_batch(["gone/repo"])
        assert results[0].error == "not_found"

    def it_handles_history_not_found_repo(gql):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"r0": None}}
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_history_batch([("o", "r", "main", "f.py")])
        assert results[0].error == "not_found"

    def it_handles_history_bad_ref(gql):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"data": {"r0": {"ref0": None}}}
        gql._client = MagicMock()
        gql._client.post.return_value = mock_resp

        results = gql.fetch_history_batch([("o", "r", "badref", "f.py")])
        assert results[0].error == "bad_ref"

    def it_queries_per_sec_and_avg_query_time(gql):
        assert gql.queries_per_sec == 0  # no queries yet
        # Simulate a query
        gql.queries = 5
        gql.total_query_time = 2.5
        gql._start_time = gql._start_time - 10  # pretend 10s elapsed
        assert gql.avg_query_time == 0.5
        assert gql.queries_per_sec > 0
