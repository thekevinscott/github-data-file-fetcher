"""Integration tests for the generic GitHub API client and CLI subcommand.

Cachetta writes real files to tmp dirs. Only external HTTP calls (httpx) are mocked.
"""

import hashlib
import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from github_data_file_fetcher.generic_client import GenericGitHubClient, _parse_retry_after
from github_data_file_fetcher.models import ApiResponse


@pytest.fixture
def cache_dir(tmp_path):
    d = tmp_path / "cache"
    d.mkdir()
    return d


@pytest.fixture(autouse=True)
def _no_sleep():
    """Patch out time.sleep in generic_client to avoid real waits in retry tests."""
    with patch("github_data_file_fetcher.generic_client.time.sleep"):
        yield


@pytest.fixture
def client(cache_dir):
    with patch("github_data_file_fetcher.generic_client.get_settings") as mock_settings:
        mock_settings.return_value = MagicMock(github_token="test-token")
        c = GenericGitHubClient(cache_dir=cache_dir)
    # Disable throttle for tests
    c._min_interval = 0
    return c


def _cache_key(endpoint, params=None):
    """Reproduce the cache key hash for verifying file existence."""
    params = params or {}
    raw = f"{endpoint}|{json.dumps(params, sort_keys=True)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _write_cache_file(cache_dir, endpoint, params, data):
    """Write a cache file in the format Cachetta expects (plain JSON)."""
    key = _cache_key(endpoint, params)
    path = cache_dir / f"{key}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f)


def _cache_file_exists(cache_dir, endpoint, params=None):
    """Check if a cache file exists for the given endpoint+params."""
    key = _cache_key(endpoint, params)
    return (cache_dir / f"{key}.json").exists()


def _mock_response(status_code=200, json_body=None, headers=None):
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.content = json.dumps(json_body).encode() if json_body is not None else b""
    resp.json.return_value = json_body
    resp.text = json.dumps(json_body) if json_body is not None else ""
    resp.headers = headers or {}
    resp.request = MagicMock()
    return resp


class TestApiResponse:
    def test_fields(self):
        r = ApiResponse(status=200, body={"key": "val"}, etag='"abc"', link="<url>; rel=next")
        assert r.status == 200
        assert r.body == {"key": "val"}
        assert r.etag == '"abc"'
        assert r.link == "<url>; rel=next"

    def test_defaults(self):
        r = ApiResponse(status=200, body=[])
        assert r.etag is None
        assert r.link is None


class TestGenericGitHubClient:
    def test_cache_hit(self, client, cache_dir):
        """Cached responses are returned without HTTP calls."""
        _write_cache_file(cache_dir, "repos/o/r", {}, {
            "status": 200,
            "body": {"id": 1},
            "etag": '"xyz"',
            "link": None,
        })
        resp = client.api("repos/o/r")
        assert resp.status == 200
        assert resp.body == {"id": 1}
        assert resp.etag == '"xyz"'

    def test_cache_miss_200(self, client, cache_dir):
        """2xx responses are cached and returned."""
        body = {"full_name": "owner/repo"}
        mock_resp = _mock_response(200, body, headers={"etag": '"e1"', "link": "<next>"})
        client._client.request = MagicMock(return_value=mock_resp)

        resp = client.api("repos/owner/repo")
        assert resp.status == 200
        assert resp.body == body
        assert resp.etag == '"e1"'
        assert resp.link == "<next>"

        # Second call hits cache
        client._client.request.reset_mock()
        resp2 = client.api("repos/owner/repo")
        assert resp2.body == body
        client._client.request.assert_not_called()

        # Verify file was written
        assert _cache_file_exists(cache_dir, "repos/owner/repo")

    def test_only_caches_2xx(self, client, cache_dir):
        """Non-2xx responses are NOT cached."""
        resp_404 = _mock_response(404, {"message": "Not Found"})
        client._client.request = MagicMock(return_value=resp_404)

        with pytest.raises(httpx.HTTPStatusError):
            client.api("repos/gone/repo")

        assert not _cache_file_exists(cache_dir, "repos/gone/repo")

    def test_skip_cache(self, client, cache_dir):
        """skip_cache bypasses cache reads but still writes."""
        _write_cache_file(cache_dir, "repos/o/r", {}, {
            "status": 200,
            "body": {"cached": True},
        })
        fresh_body = {"cached": False}
        mock_resp = _mock_response(200, fresh_body)
        client._client.request = MagicMock(return_value=mock_resp)

        resp = client.api("repos/o/r", skip_cache=True)
        assert resp.body == {"cached": False}
        client._client.request.assert_called_once()

    def test_204_no_content(self, client):
        """204 responses return empty body."""
        mock_resp = _mock_response(204, None)
        client._client.request = MagicMock(return_value=mock_resp)

        resp = client.api("repos/o/r/issues/1", method="DELETE")
        assert resp.status == 204
        assert resp.body == {}

    def test_non_get_not_cached(self, client, cache_dir):
        """POST/PUT/DELETE responses are not cached."""
        body = {"id": 42}
        mock_resp = _mock_response(201, body)
        client._client.request = MagicMock(return_value=mock_resp)

        resp = client.api("repos/o/r/forks", method="POST")
        assert resp.status == 201

        assert not _cache_file_exists(cache_dir, "repos/o/r/forks")

    def test_endpoint_normalization(self, client):
        """Both 'repos/o/r' and '/repos/o/r' work."""
        body = {"ok": True}
        mock_resp = _mock_response(200, body)
        calls = []

        def capture_request(method, url, **kw):
            calls.append(url)
            return mock_resp

        client._client.request = MagicMock(side_effect=capture_request)

        client.api("repos/o/r")
        # Different endpoint string means different cache key, so second call also hits API
        client.api("/repos/o/r")

        assert calls[0] == "https://api.github.com/repos/o/r"
        assert calls[0] == calls[1]

    def test_rate_limit_retry(self, client):
        """429 triggers retry with eventual success."""
        rate_limited = _mock_response(429, {"message": "rate limit"}, headers={"retry-after": "0"})
        success = _mock_response(200, {"ok": True})
        client._client.request = MagicMock(side_effect=[rate_limited, success])

        resp = client.api("repos/o/r")
        assert resp.status == 200
        assert client._client.request.call_count == 2

    def test_403_rate_limit_retry(self, client):
        """403 with 'rate limit' in body triggers retry."""
        rate_limited = _mock_response(403, {"message": "API rate limit exceeded"})
        rate_limited.text = "API rate limit exceeded"
        rate_limited.headers = {"retry-after": "0"}
        success = _mock_response(200, {"ok": True})
        client._client.request = MagicMock(side_effect=[rate_limited, success])

        resp = client.api("repos/o/r")
        assert resp.status == 200

    def test_5xx_retry(self, client):
        """5xx errors are retried."""
        server_error = _mock_response(502, {"message": "Bad Gateway"})
        success = _mock_response(200, {"ok": True})
        client._client.request = MagicMock(side_effect=[server_error, success])

        resp = client.api("repos/o/r")
        assert resp.status == 200

    @patch("github_data_file_fetcher.generic_client.MAX_RETRIES", 3)
    def test_retries_exhausted(self, client):
        """RuntimeError after MAX_RETRIES."""
        server_error = _mock_response(502, {"message": "Bad Gateway"})
        client._client.request = MagicMock(return_value=server_error)

        with pytest.raises(RuntimeError, match="failed after"):
            client.api("repos/o/r")

    def test_connection_error_retry(self, client):
        """Transient connection errors are retried."""
        success = _mock_response(200, {"ok": True})
        client._client.request = MagicMock(
            side_effect=[httpx.ConnectError("connection failed"), success]
        )

        resp = client.api("repos/o/r")
        assert resp.status == 200

    def test_params_in_cache_key(self, client):
        """Different params produce different cache entries."""
        body1 = {"page": 1}
        body2 = {"page": 2}
        mock1 = _mock_response(200, body1)
        mock2 = _mock_response(200, body2)
        client._client.request = MagicMock(side_effect=[mock1, mock2])

        resp1 = client.api("search/code", params={"q": "test", "page": "1"})
        resp2 = client.api("search/code", params={"q": "test", "page": "2"})
        assert resp1.body == body1
        assert resp2.body == body2
        assert client._client.request.call_count == 2


class TestParseRetryAfter:
    def test_present(self):
        resp = MagicMock()
        resp.headers = {"retry-after": "30"}
        assert _parse_retry_after(resp) == 30.0

    def test_missing(self):
        resp = MagicMock()
        resp.headers = {}
        assert _parse_retry_after(resp) is None

    def test_invalid(self):
        resp = MagicMock()
        resp.headers = {"retry-after": "not-a-number"}
        assert _parse_retry_after(resp) is None


class TestGraphqlGenericMethod:
    """Tests for GraphQLClient.graphql() generic method."""

    def test_cache_hit(self, cache_dir):
        """Pre-populated cache file is returned without API call."""
        # Write cache file using the graphql cache path format
        params = {"query": "{ viewer { login } }", "variables": {}}
        raw = f"graphql|{json.dumps(params, sort_keys=True)}"
        key = hashlib.sha256(raw.encode()).hexdigest()[:16]
        cache_file = cache_dir / f"{key}.json"
        cache_file.write_text(json.dumps({"data": {"viewer": {"login": "test-user"}}}))

        with patch("github_data_file_fetcher.graphql.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(github_token="test-token")
            from github_data_file_fetcher.graphql import GraphQLClient
            gql = GraphQLClient(cache_dir=cache_dir)

        result = gql.graphql("{ viewer { login } }")
        assert result == {"data": {"viewer": {"login": "test-user"}}}

    def test_cache_miss(self, cache_dir):
        with patch("github_data_file_fetcher.graphql.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(github_token="test-token")
            from github_data_file_fetcher.graphql import GraphQLClient
            gql = GraphQLClient(cache_dir=cache_dir)

        body = {"data": {"repository": {"name": "test"}}}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = body
        gql._client.post = MagicMock(return_value=mock_resp)
        gql._min_interval = 0

        result = gql.graphql("{ repository(owner:\"o\", name:\"r\") { name } }")
        assert result["data"]["repository"]["name"] == "test"

        # Verify cache file was written
        params = {"query": "{ repository(owner:\"o\", name:\"r\") { name } }", "variables": {}}
        raw = f"graphql|{json.dumps(params, sort_keys=True)}"
        key = hashlib.sha256(raw.encode()).hexdigest()[:16]
        assert (cache_dir / f"{key}.json").exists()

    def test_error_not_cached(self, cache_dir):
        with patch("github_data_file_fetcher.graphql.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(github_token="test-token")
            from github_data_file_fetcher.graphql import GraphQLClient
            gql = GraphQLClient(cache_dir=cache_dir)

        body = {"errors": [{"message": "some error"}]}
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = body
        gql._client.post = MagicMock(return_value=mock_resp)
        gql._min_interval = 0

        result = gql.graphql("{ bad query }")
        assert "errors" in result

        # No cache file written
        params = {"query": "{ bad query }", "variables": {}}
        raw = f"graphql|{json.dumps(params, sort_keys=True)}"
        key = hashlib.sha256(raw.encode()).hexdigest()[:16]
        assert not (cache_dir / f"{key}.json").exists()


class TestCliApiSubcommand:
    """Tests for the `api` CLI subcommand."""

    def test_rest_call(self, capsys):
        body = {"full_name": "owner/repo"}
        mock_resp = ApiResponse(status=200, body=body)

        with patch("github_data_file_fetcher.generic_client.get_generic_client") as mock_get:
            mock_client = MagicMock()
            mock_client.api.return_value = mock_resp
            mock_get.return_value = mock_client

            from github_data_file_fetcher.cli import main
            with patch("sys.argv", ["prog", "api", "repos/owner/repo"]):
                main()

        out = capsys.readouterr().out
        assert json.loads(out) == body

    def test_rest_with_params(self):
        body = {"total_count": 0, "items": []}
        mock_resp = ApiResponse(status=200, body=body)

        with patch("github_data_file_fetcher.generic_client.get_generic_client") as mock_get:
            mock_client = MagicMock()
            mock_client.api.return_value = mock_resp
            mock_get.return_value = mock_client

            from github_data_file_fetcher.cli import main
            with patch("sys.argv", ["prog", "api", "search/code",
                                    "--param", "q=filename:SKILL.md",
                                    "--param", "per_page=100"]):
                main()

        mock_client.api.assert_called_once_with(
            "search/code",
            params={"q": "filename:SKILL.md", "per_page": "100"},
            method="GET",
        )

    def test_graphql_mode(self, capsys):
        gql_result = {"data": {"viewer": {"login": "user"}}}

        with patch("github_data_file_fetcher.graphql.GraphQLClient") as MockGQL:
            mock_gql = MagicMock()
            mock_gql.graphql.return_value = gql_result
            MockGQL.return_value = mock_gql

            from github_data_file_fetcher.cli import main
            with patch("sys.argv", ["prog", "api", "graphql", "--graphql",
                                    "--query", "{ viewer { login } }"]):
                main()

        out = capsys.readouterr().out
        assert json.loads(out) == gql_result

    def test_skip_cache_flag(self):
        mock_resp = ApiResponse(status=200, body={})

        with patch("github_data_file_fetcher.generic_client.get_generic_client") as mock_get:
            mock_client = MagicMock()
            mock_client.api.return_value = mock_resp
            mock_get.return_value = mock_client

            from github_data_file_fetcher.cli import main
            with patch("sys.argv", ["prog", "api", "repos/o/r", "--skip-cache"]):
                main()

        mock_get.assert_called_once_with(skip_cache=True)
