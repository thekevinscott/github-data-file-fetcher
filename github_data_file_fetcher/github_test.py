"""Unit tests for GitHub client."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from github import GithubException, RateLimitExceededException

from .github import GitHubClient


def describe_GitHubClient():
    @pytest.fixture
    def client(tmp_path: Path):
        return GitHubClient(cache_dir=tmp_path / ".cache")

    def describe_cache():
        def it_returns_none_on_cache_miss(client: GitHubClient):
            result = client.cache.get("search/code", {"q": "test"})
            assert result is None

        def it_caches_and_retrieves(client: GitHubClient):
            endpoint = "search/code"
            params = {"q": "test", "page": 1}
            data = {"total_count": 5, "items": [{"id": 1}]}

            client.cache.set(endpoint, params, data)
            result = client.cache.get(endpoint, params)

            assert result == data

        def it_creates_cache_dir_if_missing(client: GitHubClient):
            assert not client.cache.cache_dir.exists()

            client.cache.set("test", {}, {"data": 1})

            assert client.cache.cache_dir.exists()

        def it_generates_different_keys_for_different_params(client: GitHubClient):
            key1 = client.cache._key("search/code", {"q": "foo"})
            key2 = client.cache._key("search/code", {"q": "bar"})

            assert key1 != key2

        def it_generates_same_key_regardless_of_param_order(client: GitHubClient):
            key1 = client.cache._key("search/code", {"a": 1, "b": 2})
            key2 = client.cache._key("search/code", {"b": 2, "a": 1})

            assert key1 == key2

    def describe_github_client_init():
        def it_uses_token_from_env_when_available(client: GitHubClient):
            with patch.dict("os.environ", {"GITHUB_TOKEN": "test-token"}):
                # Force re-init
                client._github = None
                github = client.github

                # Check that auth was set up (we can't easily inspect the token)
                assert github is not None

        def it_works_without_token(client: GitHubClient):
            with patch.dict("os.environ", {}, clear=True):
                client._github = None
                github = client.github

                assert github is not None

    def describe_search_code():
        @pytest.fixture
        def mock_github(client: GitHubClient):
            mock = MagicMock()
            client._github = mock
            return mock

        def it_returns_cached_result_without_api_call(client: GitHubClient, mock_github: MagicMock):
            # Pre-populate cache
            client.cache.set(
                "search/code",
                {"q": "test", "per_page": 100, "page": 1},
                {"total_count": 5, "items": [{"sha": "abc"}]},
            )

            result = client.search_code("test")

            assert result == {"total_count": 5, "items": [{"sha": "abc"}]}
            mock_github.search_code.assert_not_called()

        def it_calls_github_api_on_cache_miss(client: GitHubClient, mock_github: MagicMock):
            mock_item = MagicMock()
            mock_item.sha = "abc123"
            mock_item.name = "SKILL.md"
            mock_item.path = "path/to/SKILL.md"
            mock_item.html_url = "https://github.com/owner/repo/blob/main/SKILL.md"
            mock_item.repository.full_name = "owner/repo"

            mock_results = MagicMock()
            mock_results.totalCount = 1
            mock_results.get_page.return_value = [mock_item]
            mock_github.search_code.return_value = mock_results

            result = client.search_code("filename:SKILL.md")

            assert result["total_count"] == 1
            assert len(result["items"]) == 1
            assert result["items"][0]["sha"] == "abc123"
            mock_github.search_code.assert_called_once_with(query="filename:SKILL.md")
            mock_results.get_page.assert_called_once_with(0)  # Page 1 = index 0

        def it_handles_pagination(client: GitHubClient, mock_github: MagicMock):
            # Create mock items for page 2 (items 100-149)
            page2_items = []
            for i in range(100, 150):
                item = MagicMock()
                item.sha = f"sha{i}"
                item.name = "SKILL.md"
                item.path = f"path{i}/SKILL.md"
                item.html_url = f"https://github.com/owner/repo{i}/blob/main/SKILL.md"
                item.repository.full_name = f"owner/repo{i}"
                page2_items.append(item)

            mock_results = MagicMock()
            mock_results.totalCount = 150
            mock_results.get_page.return_value = page2_items
            mock_github.search_code.return_value = mock_results

            # Get page 2 (items 100-149)
            result = client.search_code("test", per_page=100, page=2)

            assert result["total_count"] == 150
            assert len(result["items"]) == 50
            assert result["items"][0]["sha"] == "sha100"
            mock_results.get_page.assert_called_once_with(1)  # Page 2 = index 1

        def it_returns_full_page_when_per_page_items_available(
            client: GitHubClient, mock_github: MagicMock
        ):
            """CRITICAL: Must return per_page items via get_page()."""
            # Create 100 items for page 1
            page1_items = []
            for i in range(100):
                item = MagicMock()
                item.sha = f"sha{i}"
                item.name = "SKILL.md"
                item.path = f"path{i}/SKILL.md"
                item.html_url = f"https://github.com/owner/repo{i}/blob/main/SKILL.md"
                item.repository.full_name = f"owner/repo{i}"
                page1_items.append(item)

            mock_results = MagicMock()
            mock_results.totalCount = 500
            mock_results.get_page.return_value = page1_items
            mock_github.search_code.return_value = mock_results

            result = client.search_code("test", per_page=100, page=1)

            # MUST return 100 items from get_page()
            assert len(result["items"]) == 100, (
                f"Expected 100 items but got {len(result['items'])}. "
                "This indicates get_page is not working correctly."
            )
            mock_results.get_page.assert_called_once_with(0)  # Page 1 = index 0

        def it_caches_successful_response(client: GitHubClient, mock_github: MagicMock):
            mock_results = MagicMock()
            mock_results.totalCount = 0
            mock_results.get_page.return_value = []
            mock_github.search_code.return_value = mock_results

            client.search_code("new_query")

            # Should be cached now
            cached = client.cache.get("search/code", {"q": "new_query", "per_page": 100, "page": 1})
            assert cached == {"total_count": 0, "items": []}

        def it_retries_on_rate_limit(client: GitHubClient, mock_github: MagicMock):
            mock_github.rate_limiting_resettime = 0

            # First call raises rate limit, second succeeds
            mock_results = MagicMock()
            mock_results.totalCount = 0
            mock_results.get_page.return_value = []

            mock_github.search_code.side_effect = [
                RateLimitExceededException(403, {"message": "rate limit"}, {}),
                mock_results,
            ]

            with patch("time.sleep"):
                result = client.search_code("test")

            assert result == {"total_count": 0, "items": []}
            assert mock_github.search_code.call_count == 2

        def it_returns_empty_on_422(client: GitHubClient, mock_github: MagicMock):
            mock_github.search_code.side_effect = GithubException(
                422, {"message": "Validation Failed"}, {}
            )

            result = client.search_code("test")

            assert result == {"total_count": 0, "items": []}

    def describe_get_file_content():
        @pytest.fixture
        def mock_github(client: GitHubClient):
            mock = MagicMock()
            client._github = mock
            return mock

        def it_returns_cached_result_without_api_call(client: GitHubClient, mock_github: MagicMock):
            client.cache.set(
                "contents",
                {"owner": "owner", "repo": "repo", "path": "file.md", "ref": None},
                {"content": "dGVzdA==", "sha": "abc"},
            )

            result = client.get_file_content("owner", "repo", "file.md")

            assert result == {"content": "dGVzdA==", "sha": "abc"}
            mock_github.get_repo.assert_not_called()

        def it_fetches_content_from_github(client: GitHubClient, mock_github: MagicMock):
            mock_contents = MagicMock()
            mock_contents.content = "dGVzdCBjb250ZW50"  # "test content" base64
            mock_contents.encoding = "base64"
            mock_contents.sha = "abc123"
            mock_contents.size = 12
            mock_contents.name = "SKILL.md"
            mock_contents.path = "path/to/SKILL.md"

            mock_repo = MagicMock()
            mock_repo.get_contents.return_value = mock_contents
            mock_github.get_repo.return_value = mock_repo

            result = client.get_file_content("owner", "repo", "path/to/SKILL.md")

            assert result["content"] == "dGVzdCBjb250ZW50"
            assert result["sha"] == "abc123"
            mock_github.get_repo.assert_called_once_with("owner/repo")
            mock_repo.get_contents.assert_called_once_with("path/to/SKILL.md", ref=None)

        def it_includes_ref_when_provided(client: GitHubClient, mock_github: MagicMock):
            mock_contents = MagicMock()
            mock_contents.content = "dGVzdA=="
            mock_contents.encoding = "base64"
            mock_contents.sha = "abc"
            mock_contents.size = 4
            mock_contents.name = "file.md"
            mock_contents.path = "file.md"

            mock_repo = MagicMock()
            mock_repo.get_contents.return_value = mock_contents
            mock_github.get_repo.return_value = mock_repo

            client.get_file_content("owner", "repo", "file.md", ref="main")

            mock_repo.get_contents.assert_called_once_with("file.md", ref="main")

        def it_raises_file_not_found_on_404(client: GitHubClient, mock_github: MagicMock):
            mock_github.get_repo.side_effect = GithubException(404, {"message": "Not Found"}, {})

            with pytest.raises(FileNotFoundError, match="File not found"):
                client.get_file_content("owner", "repo", "missing.md")

        def it_raises_file_not_found_for_directories(client: GitHubClient, mock_github: MagicMock):
            # get_contents returns a list for directories
            mock_repo = MagicMock()
            mock_repo.get_contents.return_value = [MagicMock(), MagicMock()]
            mock_github.get_repo.return_value = mock_repo

            with pytest.raises(FileNotFoundError, match="Path is a directory"):
                client.get_file_content("owner", "repo", "some/directory")

        def it_retries_on_rate_limit(client: GitHubClient, mock_github: MagicMock):
            mock_github.rate_limiting_resettime = 0

            mock_contents = MagicMock()
            mock_contents.content = "dGVzdA=="
            mock_contents.encoding = "base64"
            mock_contents.sha = "abc"
            mock_contents.size = 4
            mock_contents.name = "file.md"
            mock_contents.path = "file.md"

            mock_repo = MagicMock()
            mock_repo.get_contents.side_effect = [
                RateLimitExceededException(403, {"message": "rate limit"}, {}),
                mock_contents,
            ]
            mock_github.get_repo.return_value = mock_repo

            with patch("time.sleep"):
                result = client.get_file_content("owner", "repo", "file.md")

            assert result["sha"] == "abc"
            assert mock_repo.get_contents.call_count == 2

        def it_caches_successful_response(client: GitHubClient, mock_github: MagicMock):
            mock_contents = MagicMock()
            mock_contents.content = "dGVzdA=="
            mock_contents.encoding = "base64"
            mock_contents.sha = "abc"
            mock_contents.size = 4
            mock_contents.name = "file.md"
            mock_contents.path = "file.md"

            mock_repo = MagicMock()
            mock_repo.get_contents.return_value = mock_contents
            mock_github.get_repo.return_value = mock_repo

            client.get_file_content("owner", "repo", "file.md")

            cached = client.cache.get(
                "contents",
                {"owner": "owner", "repo": "repo", "path": "file.md", "ref": None},
            )
            assert cached is not None
            assert cached["sha"] == "abc"

    def describe_skip_cache():

        def it_bypasses_cache_reads_when_skip_cache_true(tmp_path: Path):
            client = GitHubClient(cache_dir=tmp_path / ".skip_cache", skip_cache=True)
            client.cache.set("test", {"k": 1}, {"data": "stored"})

            # skip_cache client returns None even though data is on disk
            assert client.cache.get("test", {"k": 1}) is None

            # Normal client can read the same data
            from .github import Cache
            normal = Cache(tmp_path / ".skip_cache", skip_cache=False)
            assert normal.get("test", {"k": 1}) == {"data": "stored"}

    def describe_corrupt_cache():

        def it_returns_none_for_corrupt_json(client: GitHubClient):
            client.cache.set("test", {"k": "v"}, {"data": "good"})
            key = client.cache._key("test", {"k": "v"})
            path = client.cache.cache_dir / f"{key}.json"
            path.write_text("not valid json {{{")

            assert client.cache.get("test", {"k": "v"}) is None

        def it_returns_none_for_empty_file(client: GitHubClient):
            client.cache.set("test", {"k": 1}, {"data": "ok"})
            key = client.cache._key("test", {"k": 1})
            path = client.cache.cache_dir / f"{key}.json"
            path.write_text("")

            assert client.cache.get("test", {"k": 1}) is None

        def it_returns_none_for_binary_garbage(client: GitHubClient):
            client.cache.set("test", {"k": 2}, {"data": "ok"})
            key = client.cache._key("test", {"k": 2})
            path = client.cache.cache_dir / f"{key}.json"
            path.write_bytes(b"\x00\x01\x02\xff\xfe")

            assert client.cache.get("test", {"k": 2}) is None

    def describe_cache_key_stability():

        def it_is_stable_across_instances(tmp_path: Path):
            from .github import Cache
            c1 = Cache(tmp_path / ".keys")
            c2 = Cache(tmp_path / ".keys")
            assert c1._key("ep", {"x": 1}) == c2._key("ep", {"x": 1})

        def it_differs_across_endpoints(client: GitHubClient):
            k1 = client.cache._key("search/code", {"q": "t"})
            k2 = client.cache._key("contents", {"q": "t"})
            assert k1 != k2
