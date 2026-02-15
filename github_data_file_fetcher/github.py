"""GitHub API client using PyGithub with caching and rate limiting."""

import hashlib
import json
import threading
import time
from pathlib import Path

from github import Auth, Github, GithubException, RateLimitExceededException

from .settings import get_settings

DEFAULT_CACHE_DIR = Path.home() / ".cache/github-data-file-fetcher"

# Rate limit backoff settings
BACKOFF_FACTOR = 1.5
MAX_RETRIES = 30

# Steady-state throttle: 1.3 req/sec = ~4,680/hour (under 5K limit)
# Avoids bursts that trigger GitHub's secondary/abuse rate limits
REQUESTS_PER_SECOND = 1.3


class Cache:
    """Simple file-based cache for API responses."""

    def __init__(self, cache_dir: Path, skip_cache: bool = False):
        self.cache_dir = cache_dir
        self.skip_cache = skip_cache
        self.hits = 0

    def _key(self, endpoint: str, params: dict) -> str:
        """Generate cache key for an API call."""
        key = f"{endpoint}|{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    def get(self, endpoint: str, params: dict) -> dict | None:
        """Get cached API response."""
        if self.skip_cache:
            return None
        key = self._key(endpoint, params)
        path = self.cache_dir / f"{key}.json"
        if path.exists():
            try:
                with open(path) as f:
                    data = json.load(f)
                self.hits += 1
                return data
            except (json.JSONDecodeError, OSError, UnicodeDecodeError):
                return None
        return None

    def set(self, endpoint: str, params: dict, data: dict):
        """Cache an API response."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self.cache_dir / f"{self._key(endpoint, params)}.json"
        with open(path, "w") as f:
            json.dump(data, f)


class GitHubClient:
    """GitHub API client using PyGithub with caching.

    Auth is handled via GITHUB_TOKEN environment variable.
    PyGithub handles rate limiting automatically.
    """

    def __init__(self, cache_dir: Path | None = None, skip_cache: bool = False):
        self.cache = Cache(cache_dir or DEFAULT_CACHE_DIR, skip_cache=skip_cache)
        self._github: Github | None = None
        self._rate_limit_hits = 0
        self._rate_limit_reset = 0  # unix timestamp when rate limit resets
        self.api_retries = 0  # count of transient API errors that were retried
        # Throttle: space API calls evenly to avoid secondary rate limits
        self._throttle_lock = threading.Lock()
        self._last_request_time = 0.0
        self._min_interval = 1.0 / REQUESTS_PER_SECOND
        self._repo_cache: dict[str, object] = {}  # cache repo objects to avoid extra API calls
        self._repo_cache_lock = threading.Lock()

    @property
    def github(self) -> Github:
        """Lazy-initialize the GitHub client."""
        if self._github is None:
            settings = get_settings()
            if not settings.github_token:
                raise Exception("GITHUB_TOKEN is not set")
            auth = Auth.Token(settings.github_token)
            self._github = Github(auth=auth, retry=3, per_page=100)
            # Guard against PyGithub's default of 30 items per page
            assert self._github.per_page == 100, (
                f"per_page must be 100 to avoid pagination bugs, got {self._github.per_page}"
            )
        return self._github

    @property
    def rate_limit_waiting(self) -> int:
        """Seconds until rate limit resets, 0 if not limited."""
        return max(0, int(self._rate_limit_reset - time.time()))

    def _throttle(self) -> None:
        """Wait if needed to maintain steady request rate."""
        with self._throttle_lock:
            now = time.time()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request_time = time.time()

    def _handle_rate_limit(self, e: RateLimitExceededException) -> None:
        """Handle rate limit by waiting until reset."""
        self._rate_limit_hits += 1
        reset_time = self.github.rate_limiting_resettime
        self._rate_limit_reset = max(self._rate_limit_reset, reset_time + 1)
        while time.time() < self._rate_limit_reset:
            time.sleep(1)

    def search_code(
        self,
        query: str,
        per_page: int = 100,
        page: int = 1,
    ) -> dict:
        """Search code on GitHub.

        Returns dict with 'total_count' and 'items' keys to match previous API.

        Uses get_page() for direct page access (single API call per page).
        """
        cached = self.cache.get("search/code", {"q": query, "per_page": per_page, "page": page})
        if cached is not None:
            return cached

        # Convert + to space for PyGithub (gh CLI handled URL encoding internally)
        api_query = query.replace("+", " ")

        other_errors = 0
        while True:
            try:
                self._throttle()
                # PyGithub's search_code returns a PaginatedList
                results = self.github.search_code(query=api_query)

                # get_page() makes a direct API call for that specific page
                # Page is 0-indexed in get_page (0 = first page)
                page_items = results.get_page(page - 1)

                # totalCount is populated after get_page call
                total_count = results.totalCount

                # Convert to our dict format
                items = []
                for item in page_items:
                    items.append(
                        {
                            "sha": item.sha,
                            "name": item.name,
                            "path": item.path,
                            "html_url": item.html_url,
                            "repository": {
                                "full_name": item.repository.full_name,
                            },
                        }
                    )

                result = {"total_count": total_count, "items": items}
                # Don't cache empty responses on later pages -- likely transient GitHub issue
                if items or page == 1:
                    self.cache.set(
                        "search/code", {"q": query, "per_page": per_page, "page": page}, result
                    )
                return result

            except RateLimitExceededException as e:
                self._handle_rate_limit(e)
                continue
            except GithubException as e:
                if e.status == 403 and "rate limit" in str(e).lower():
                    self._handle_rate_limit(RateLimitExceededException(e.status, e.data, e.headers))
                    continue
                elif e.status == 422:
                    # Pagination limit reached
                    return {"total_count": 0, "items": []}
                elif e.status == 429:
                    # Rate limit - wait and retry
                    self._handle_rate_limit(RateLimitExceededException(e.status, e.data, e.headers))
                    continue
                else:
                    other_errors += 1
                    if other_errors >= MAX_RETRIES:
                        raise RuntimeError(
                            f"Max retries exceeded for query: {query[:50]}..."
                        ) from e
                    print(f"API error: {e}, retrying ({other_errors}/{MAX_RETRIES})...")
                    time.sleep(5 * BACKOFF_FACTOR**other_errors)
                    continue

    def get_file_content(
        self,
        owner: str,
        repo: str,
        path: str,
        ref: str | None = None,
    ) -> dict:
        """Get file content from a repository.

        Returns dict with 'content' key (base64 encoded) to match previous API.
        """
        cache_params = {"owner": owner, "repo": repo, "path": path, "ref": ref}
        cached = self.cache.get("contents", cache_params)
        if cached is not None:
            return cached

        for attempt in range(MAX_RETRIES):
            try:
                repo_key = f"{owner}/{repo}"
                with self._repo_cache_lock:
                    repo_obj = self._repo_cache.get(repo_key)
                if repo_obj is None:
                    self._throttle()
                    repo_obj = self.github.get_repo(repo_key)
                    with self._repo_cache_lock:
                        self._repo_cache[repo_key] = repo_obj
                self._throttle()
                contents = repo_obj.get_contents(path, ref=ref)

                # get_contents returns ContentFile for files
                if isinstance(contents, list):
                    # It's a directory, not a file
                    raise FileNotFoundError(f"Path is a directory: {path}")

                # Symlink: resolve if relative, skip if absolute
                if contents.type == "symlink":
                    target = getattr(contents, "target", None)
                    if not target or target.startswith("/"):
                        self.cache.set("contents", cache_params, {"error": "unresolvable_symlink"})
                        return {"error": "unresolvable_symlink"}
                    # Resolve relative symlink within repo
                    import posixpath
                    resolved = posixpath.normpath(posixpath.join(posixpath.dirname(path), target))
                    return self.get_file_content(owner, repo, resolved, ref=ref)

                if contents.content is None:
                    self.cache.set("contents", cache_params, {"error": "no_content"})
                    return {"error": "no_content"}

                result = {
                    "content": contents.content,  # Already base64 encoded
                    "encoding": contents.encoding,
                    "sha": contents.sha,
                    "size": contents.size,
                    "name": contents.name,
                    "path": contents.path,
                }
                self.cache.set("contents", cache_params, result)
                return result

            except RateLimitExceededException as e:
                self._handle_rate_limit(e)
                continue
            except GithubException as e:
                if e.status == 403 and "rate limit" in str(e).lower():
                    self._handle_rate_limit(RateLimitExceededException(e.status, e.data, e.headers))
                    continue
                elif e.status == 404:
                    self.cache.set("contents", cache_params, {"error": "not_found"})
                    raise FileNotFoundError(f"File not found: {owner}/{repo}/{path}") from e
                elif e.status == 429:
                    self._handle_rate_limit(RateLimitExceededException(e.status, e.data, e.headers))
                    continue
                else:
                    self.api_retries += 1
                    time.sleep(5 * BACKOFF_FACTOR**attempt)
                    continue

        raise RuntimeError(f"Max retries exceeded fetching {owner}/{repo}/{path}")


# Client instances keyed by config
_clients: dict[tuple, GitHubClient] = {}


def get_client(cache_dir: Path | None = None, skip_cache: bool = False) -> GitHubClient:
    """Get or create a GitHub client with the given configuration."""
    key = (str(cache_dir) if cache_dir else None, skip_cache)
    if key not in _clients:
        _clients[key] = GitHubClient(cache_dir, skip_cache=skip_cache)
    return _clients[key]
