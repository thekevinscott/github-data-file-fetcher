"""Generic cached GitHub REST API client using httpx + Cachetta."""

import hashlib
import json
import time
from datetime import timedelta
from pathlib import Path

import httpx
from cachetta import Cachetta

from .github import BACKOFF_FACTOR, DEFAULT_CACHE_DIR, MAX_RETRIES, REQUESTS_PER_SECOND
from .models import ApiResponse
from .settings import get_settings

API_BASE = "https://api.github.com"
DEFAULT_DURATION = timedelta(days=30)


def _api_cache_path(endpoint, params=None):
    """Generate cache file path from endpoint and params."""
    params = params or {}
    raw = f"{endpoint}|{json.dumps(params, sort_keys=True)}"
    key = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return DEFAULT_CACHE_DIR / f"{key}.json"


_api_cache = Cachetta(path=_api_cache_path, duration=DEFAULT_DURATION)
_api_cache_skip_read = _api_cache.copy(read=False)


class _RateLimitError(Exception):
    def __init__(self, wait):
        self.wait = wait


class _RetryableError(Exception):
    pass


class GenericGitHubClient:
    """Thin cached client for arbitrary GitHub REST API endpoints."""

    def __init__(self, cache_dir=None, skip_cache=False):
        settings = get_settings()
        if not settings.github_token:
            raise RuntimeError("GITHUB_TOKEN is not set")
        self._client = httpx.Client(
            headers={
                "Authorization": f"bearer {settings.github_token}",
                "Accept": "application/vnd.github+json",
            },
            timeout=30.0,
        )
        self._last_request_time = 0.0
        self._min_interval = 1.0 / REQUESTS_PER_SECOND
        self._skip_cache = skip_cache

        cache_dir = cache_dir or DEFAULT_CACHE_DIR

        # Pure fetch function -- no retry, no throttle, no cache logic.
        # Cachetta handles caching; exceptions propagate (not cached).
        def _do_fetch(endpoint, params=None):
            ep = endpoint if endpoint.startswith("/") else f"/{endpoint}"
            url = f"{API_BASE}{ep}"
            resp = self._client.request("GET", url, params=params)

            if resp.status_code == 429 or (
                resp.status_code == 403 and "rate limit" in resp.text.lower()
            ):
                wait = _parse_retry_after(resp) or 5
                raise _RateLimitError(wait)

            if resp.status_code >= 500:
                raise _RetryableError()

            if 200 <= resp.status_code < 300:
                return {
                    "status": resp.status_code,
                    "body": resp.json() if resp.content else {},
                    "etag": resp.headers.get("etag"),
                    "link": resp.headers.get("link"),
                }

            # Client error -- not retryable, not cached
            raise httpx.HTTPStatusError(
                f"GitHub API error {resp.status_code}",
                request=resp.request,
                response=resp,
            )

        # Configure cache with custom dir if needed
        if cache_dir != DEFAULT_CACHE_DIR:
            def _custom_path(endpoint, params=None):
                params = params or {}
                raw = f"{endpoint}|{json.dumps(params, sort_keys=True)}"
                key = hashlib.sha256(raw.encode()).hexdigest()[:16]
                return Path(cache_dir) / f"{key}.json"

            cache = Cachetta(path=_custom_path, duration=DEFAULT_DURATION)
            cache_skip_read = cache.copy(read=False)
        else:
            cache = _api_cache
            cache_skip_read = _api_cache_skip_read

        self._cached_fetch = cache(_do_fetch)
        self._skip_read_fetch = cache_skip_read(_do_fetch)
        self._raw_fetch = _do_fetch

    def _throttle(self):
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_time = time.time()

    def api(self, endpoint, params=None, method="GET", skip_cache=False):
        """Make a cached GitHub REST API call.

        Args:
            endpoint: API path, e.g. "repos/owner/repo/contents/path"
            params: Query parameters dict
            method: HTTP method (default GET)
            skip_cache: Skip reading cache for this call (still writes)

        Returns:
            ApiResponse with status, body, etag, and link fields.
        """
        params = params or {}

        for attempt in range(MAX_RETRIES):
            self._throttle()
            try:
                if method != "GET":
                    data = self._raw_fetch(endpoint, params)
                elif skip_cache or self._skip_cache:
                    data = self._skip_read_fetch(endpoint, params)
                else:
                    data = self._cached_fetch(endpoint, params)

                return ApiResponse(
                    status=data["status"],
                    body=data["body"],
                    etag=data.get("etag"),
                    link=data.get("link"),
                )
            except _RateLimitError as e:
                time.sleep(BACKOFF_FACTOR**attempt * e.wait)
                continue
            except _RetryableError:
                time.sleep(BACKOFF_FACTOR**attempt)
                continue
            except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError):
                time.sleep(BACKOFF_FACTOR**attempt)
                continue

        raise RuntimeError(f"GitHub API request failed after {MAX_RETRIES} retries: {method} {endpoint}")

    def close(self):
        self._client.close()


# Singleton cache
_generic_clients: dict[tuple, GenericGitHubClient] = {}


def get_generic_client(cache_dir=None, skip_cache=False):
    """Get or create a GenericGitHubClient with the given configuration."""
    key = (str(cache_dir) if cache_dir else None, skip_cache)
    if key not in _generic_clients:
        _generic_clients[key] = GenericGitHubClient(cache_dir, skip_cache=skip_cache)
    return _generic_clients[key]


def _parse_retry_after(resp: httpx.Response) -> float | None:
    val = resp.headers.get("retry-after")
    if val is None:
        return None
    try:
        return float(val)
    except ValueError:
        return None
