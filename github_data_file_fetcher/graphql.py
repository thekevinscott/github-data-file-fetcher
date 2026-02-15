"""GraphQL batch client for fetching data from GitHub."""

import base64
import hashlib
import json
import sys
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import httpx
from cachetta import Cachetta

from .github import DEFAULT_CACHE_DIR, Cache
from .settings import get_settings

GRAPHQL_URL = "https://api.github.com/graphql"

# GraphQL rate limit: 5,000 points/hour, secondary limit ~2,000 points/minute
# Each query costs ~1 point. 30/sec = 1,800/min, safely under secondary limit.
QUERIES_PER_SECOND = 30
MAX_RETRIES = 10
BACKOFF_FACTOR = 2
DEFAULT_DURATION = timedelta(days=30)


def _graphql_cache_path(query, variables=None):
    """Generate cache file path from query and variables."""
    params = {"query": query, "variables": variables or {}}
    raw = f"graphql|{json.dumps(params, sort_keys=True)}"
    key = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return DEFAULT_CACHE_DIR / f"{key}.json"


_graphql_cache = Cachetta(path=_graphql_cache_path, duration=DEFAULT_DURATION)


class _GraphQLErrorOnly(Exception):
    """Raised when GraphQL response has errors but no data (should not be cached)."""
    def __init__(self, body):
        self.body = body


@dataclass
class FileResult:
    owner: str
    repo: str
    path: str
    ref: str
    content_b64: str | None = None
    error: str | None = None


@dataclass
class MetadataResult:
    repo_key: str
    metadata: dict | None = None
    error: str | None = None


@dataclass
class HistoryResult:
    owner: str
    repo: str
    path: str
    commits: list[dict] | None = None
    error: str | None = None


def _make_alias(index: int) -> str:
    return f"r{index}"


def _escape_graphql_string(s: str) -> str:
    """Escape a string for use inside GraphQL double-quoted strings."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _build_query(items: list[tuple[str, str, str, str]]) -> str:
    """Build a batched GraphQL query.

    Each item is (owner, repo, ref, path).
    Groups items by (owner, repo) to avoid duplicate repository lookups,
    then creates per-file object lookups within each repo alias.
    """
    # Group by (owner, repo) -> list of (index, ref, path)
    repo_groups: dict[tuple[str, str], list[tuple[int, str, str]]] = {}
    for i, (owner, repo, ref, path) in enumerate(items):
        key = (owner, repo)
        if key not in repo_groups:
            repo_groups[key] = []
        repo_groups[key].append((i, ref, path))

    parts = []
    for repo_idx, ((owner, repo), file_list) in enumerate(repo_groups.items()):
        file_parts = []
        for item_idx, ref, path in file_list:
            expression = _escape_graphql_string(f"{ref}:{path}")
            file_parts.append(
                f'    f{item_idx}: object(expression: "{expression}") {{\n'
                f"      ... on Blob {{ text byteSize isTruncated }}\n"
                f"    }}"
            )
        files_block = "\n".join(file_parts)
        owner_esc = _escape_graphql_string(owner)
        repo_esc = _escape_graphql_string(repo)
        parts.append(
            f'  {_make_alias(repo_idx)}: repository(owner: "{owner_esc}", name: "{repo_esc}") {{\n'
            f"{files_block}\n"
            f"  }}"
        )

    return "query {\n" + "\n".join(parts) + "\n}"


class GraphQLClient:
    """GitHub GraphQL client for batched file content fetching."""

    def __init__(self, cache_dir=None, skip_cache=False):
        self.cache = Cache(cache_dir or DEFAULT_CACHE_DIR, skip_cache=skip_cache)
        self._settings = get_settings()
        if not self._settings.github_token:
            raise RuntimeError("GITHUB_TOKEN is not set")
        self._client = httpx.Client(
            headers={
                "Authorization": f"bearer {self._settings.github_token}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        self._last_query_time = 0.0
        self._min_interval = 1.0 / QUERIES_PER_SECOND
        self.queries = 0
        self.rate_limit_hits = 0
        self.retries = 0
        self.total_query_time = 0.0
        self._start_time = time.time()

        # Cachetta-backed generic graphql: closure captures self for _execute_query
        cache_dir = cache_dir or DEFAULT_CACHE_DIR
        if cache_dir != DEFAULT_CACHE_DIR:
            def _custom_path(query, variables=None):
                params = {"query": query, "variables": variables or {}}
                raw = f"graphql|{json.dumps(params, sort_keys=True)}"
                key = hashlib.sha256(raw.encode()).hexdigest()[:16]
                return Path(cache_dir) / f"{key}.json"
            gql_cache = Cachetta(path=_custom_path, duration=DEFAULT_DURATION)
        else:
            gql_cache = _graphql_cache

        def _do_graphql(query, variables=None):
            body = self._execute_query(query, variables)
            has_data = body.get("data") is not None
            if has_data:
                return body
            raise _GraphQLErrorOnly(body)

        self._cached_graphql = gql_cache(_do_graphql)
        self._raw_graphql = _do_graphql

    def _throttle(self):
        now = time.time()
        elapsed = now - self._last_query_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_query_time = time.time()

    def _log(self, msg: str):
        """Log a message above the progress line."""
        sys.stderr.write(f"\033[2K\r[graphql] {msg}\n")
        sys.stderr.flush()

    @property
    def avg_query_time(self) -> float:
        return self.total_query_time / self.queries if self.queries else 0

    @property
    def queries_per_sec(self) -> float:
        elapsed = time.time() - self._start_time
        return self.queries / elapsed if elapsed > 0 else 0

    def _execute_query(self, query: str, variables: dict | None = None) -> dict:
        """Execute a GraphQL query with retry and rate limit handling."""
        for attempt in range(MAX_RETRIES):
            self._throttle()
            t0 = time.time()
            payload = {"query": query}
            if variables is not None:
                payload["variables"] = variables
            try:
                resp = self._client.post(GRAPHQL_URL, json=payload)
            except (httpx.RemoteProtocolError, httpx.ReadError, httpx.ConnectError) as exc:
                elapsed = time.time() - t0
                self.total_query_time += elapsed
                self.queries += 1
                self.retries += 1
                wait = BACKOFF_FACTOR ** attempt
                self._log(f"{type(exc).__name__}: {exc}, retry {attempt + 1}/{MAX_RETRIES} (wait {wait}s)")
                time.sleep(wait)
                continue
            elapsed = time.time() - t0
            self.total_query_time += elapsed
            self.queries += 1

            if resp.status_code == 200:
                body = resp.json()
                # GraphQL can return 200 with errors
                if "errors" in body and not body.get("data"):
                    for err in body["errors"]:
                        if err.get("type") == "RATE_LIMITED":
                            self.rate_limit_hits += 1
                            wait = _parse_retry_after(resp) or (BACKOFF_FACTOR ** attempt * 5)
                            self._log(f"RATE LIMITED (200 body), waiting {wait:.0f}s")
                            time.sleep(wait)
                            break
                    else:
                        self.retries += 1
                        err_types = [e.get("type", "unknown") for e in body.get("errors", [])]
                        self._log(f"GraphQL errors (no data): {err_types}, retry {attempt + 1}/{MAX_RETRIES}")
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(BACKOFF_FACTOR ** attempt)
                            continue
                        return body
                return body

            if resp.status_code in (429, 403):
                self.rate_limit_hits += 1
                wait = _parse_retry_after(resp) or (BACKOFF_FACTOR ** attempt * 5)
                self._log(f"HTTP {resp.status_code}, waiting {wait:.0f}s (attempt {attempt + 1})")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                self.retries += 1
                self._log(f"HTTP {resp.status_code}, retry {attempt + 1}/{MAX_RETRIES}")
                time.sleep(BACKOFF_FACTOR ** attempt)
                continue

            # Other client errors -- don't retry
            resp.raise_for_status()

        raise RuntimeError(f"GraphQL query failed after {MAX_RETRIES} retries")

    def graphql(self, query: str, variables: dict | None = None) -> dict:
        """Execute an arbitrary GraphQL query with caching.

        Returns the full parsed JSON response body.
        Only successful responses (containing "data") are cached via Cachetta.
        """
        try:
            return self._cached_graphql(query, variables)
        except _GraphQLErrorOnly as e:
            return e.body

    def fetch_batch(
        self, items: list[tuple[str, str, str, str]]
    ) -> list[FileResult]:
        """Fetch a batch of files via GraphQL.

        Each item is (owner, repo, ref, path).
        Returns a FileResult for each item in the same order.

        Cache-compatible with REST: uses same Cache key format and base64-encodes content.
        Truncated blobs are NOT cached (they need REST fallback).
        """
        results: list[FileResult] = []
        uncached_indices: list[int] = []
        uncached_items: list[tuple[str, str, str, str]] = []

        # Phase 1: Check cache for each item
        for i, (owner, repo, ref, path) in enumerate(items):
            cache_params = {"owner": owner, "repo": repo, "path": path, "ref": ref}
            cached = self.cache.get("contents", cache_params)
            if cached is not None:
                if cached.get("error"):
                    results.append(FileResult(owner, repo, path, ref, error=cached["error"]))
                elif cached.get("content"):
                    results.append(FileResult(owner, repo, path, ref, content_b64=cached["content"]))
                else:
                    results.append(FileResult(owner, repo, path, ref, error="no_content"))
            else:
                results.append(None)  # placeholder
                uncached_indices.append(i)
                uncached_items.append((owner, repo, ref, path))

        if not uncached_items:
            return results

        # Phase 2: Build and execute GraphQL query
        query = _build_query(uncached_items)
        body = self._execute_query(query)

        data = body.get("data") or {}

        # Build reverse map: (owner, repo) -> repo_alias
        repo_groups: dict[tuple[str, str], str] = {}
        repo_idx = 0
        seen = {}
        for owner, repo, _ref, _path in uncached_items:
            key = (owner, repo)
            if key not in seen:
                seen[key] = _make_alias(repo_idx)
                repo_idx += 1
            repo_groups[(owner, repo)] = seen[key]

        # Phase 3: Map results back
        # file aliases in the query use the index within uncached_items (from _build_query's enumerate)
        for list_pos, item_idx in enumerate(uncached_indices):
            owner, repo, ref, path = uncached_items[list_pos]
            cache_params = {"owner": owner, "repo": repo, "path": path, "ref": ref}
            repo_alias = repo_groups[(owner, repo)]
            file_alias = f"f{list_pos}"

            repo_data = data.get(repo_alias)
            if repo_data is None:
                # Repository not found or access denied
                self.cache.set("contents", cache_params, {"error": "not_found"})
                results[item_idx] = FileResult(owner, repo, path, ref, error="not_found")
                continue

            blob = repo_data.get(file_alias)
            if blob is None:
                # File not found at that ref:path
                self.cache.set("contents", cache_params, {"error": "not_found"})
                results[item_idx] = FileResult(owner, repo, path, ref, error="not_found")
                continue

            text = blob.get("text")
            is_truncated = blob.get("isTruncated", False)
            byte_size = blob.get("byteSize", 0)

            if is_truncated:
                # Don't cache -- caller should fall back to REST
                results[item_idx] = FileResult(owner, repo, path, ref, error="truncated")
                continue

            if text is None:
                # Binary file or empty
                self.cache.set("contents", cache_params, {"error": "no_content"})
                results[item_idx] = FileResult(owner, repo, path, ref, error="no_content")
                continue

            # Encode to base64 to match REST cache format
            content_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
            self.cache.set("contents", cache_params, {
                "content": content_b64,
                "encoding": "base64",
                "size": byte_size,
                "path": path,
            })
            results[item_idx] = FileResult(owner, repo, path, ref, content_b64=content_b64)

        return results

    def fetch_metadata_batch(self, repo_keys: list[str]) -> list[MetadataResult]:
        """Fetch metadata for a batch of repos via GraphQL.

        Each repo_key is "owner/repo".
        Cache key matches REST: endpoint="repo_metadata", params={"repo_key": ...}.
        """
        results: list[MetadataResult] = []
        uncached_indices: list[int] = []
        uncached_keys: list[str] = []

        for i, repo_key in enumerate(repo_keys):
            cached = self.cache.get("repo_metadata", {"repo_key": repo_key})
            if cached is not None:
                if cached.get("error"):
                    results.append(MetadataResult(repo_key, error=cached["error"]))
                else:
                    results.append(MetadataResult(repo_key, metadata=cached))
            else:
                results.append(None)
                uncached_indices.append(i)
                uncached_keys.append(repo_key)

        if not uncached_keys:
            return results

        query = _build_metadata_query(uncached_keys)
        body = self._execute_query(query)
        data = body.get("data") or {}

        for list_pos, item_idx in enumerate(uncached_indices):
            repo_key = uncached_keys[list_pos]
            alias = _make_alias(list_pos)
            repo_data = data.get(alias)

            cache_params = {"repo_key": repo_key}

            if repo_data is None:
                self.cache.set("repo_metadata", cache_params, {"error": "not_found"})
                results[item_idx] = MetadataResult(repo_key, error="not_found")
                continue

            topics_nodes = repo_data.get("repositoryTopics", {}).get("nodes", [])
            topics = [n["topic"]["name"] for n in topics_nodes if n.get("topic")]

            metadata = {
                "stars": repo_data.get("stargazerCount"),
                "forks": repo_data.get("forkCount"),
                "watchers": (repo_data.get("watchers") or {}).get("totalCount"),
                "language": (repo_data.get("primaryLanguage") or {}).get("name"),
                "topics": topics,
                "created_at": repo_data.get("createdAt"),
                "updated_at": repo_data.get("updatedAt"),
                "pushed_at": repo_data.get("pushedAt"),
                "default_branch": (repo_data.get("defaultBranchRef") or {}).get("name"),
                "license": (repo_data.get("licenseInfo") or {}).get("spdxId"),
                "description": repo_data.get("description"),
            }
            self.cache.set("repo_metadata", cache_params, metadata)
            results[item_idx] = MetadataResult(repo_key, metadata=metadata)

        return results

    def fetch_history_batch(
        self, items: list[tuple[str, str, str, str]]
    ) -> list[HistoryResult]:
        """Fetch commit history for a batch of files via GraphQL.

        Each item is (owner, repo, ref, path).
        Cache key matches REST: endpoint="file_history", params={"owner", "repo", "path"}.
        """
        results: list[HistoryResult] = []
        uncached_indices: list[int] = []
        uncached_items: list[tuple[str, str, str, str]] = []

        for i, (owner, repo, ref, path) in enumerate(items):
            cache_params = {"owner": owner, "repo": repo, "path": path}
            cached = self.cache.get("file_history", cache_params)
            if cached is not None:
                if cached.get("error"):
                    results.append(HistoryResult(owner, repo, path, error=cached["error"]))
                else:
                    results.append(HistoryResult(owner, repo, path, commits=cached.get("commits", [])))
            else:
                results.append(None)
                uncached_indices.append(i)
                uncached_items.append((owner, repo, ref, path))

        if not uncached_items:
            return results

        query = _build_history_query(uncached_items)
        body = self._execute_query(query)
        data = body.get("data") or {}

        # Build same grouping as the query builder to map aliases back
        repo_groups: dict[tuple[str, str], list[tuple[int, str, str]]] = {}
        for i, (owner, repo, ref, path) in enumerate(uncached_items):
            key = (owner, repo)
            if key not in repo_groups:
                repo_groups[key] = []
            repo_groups[key].append((i, ref, path))

        # Build alias maps
        repo_alias_map: dict[tuple[str, str], str] = {}
        ref_alias_map: dict[tuple[str, str, str], str] = {}
        file_alias_map: dict[int, str] = {}  # uncached index -> file alias

        repo_idx = 0
        for (owner, repo), file_list in repo_groups.items():
            repo_alias_map[(owner, repo)] = _make_alias(repo_idx)
            # Group by ref within this repo
            ref_groups: dict[str, list[tuple[int, str]]] = {}
            for item_idx, ref, path in file_list:
                if ref not in ref_groups:
                    ref_groups[ref] = []
                ref_groups[ref].append((item_idx, path))
            for ref_idx, (ref, files) in enumerate(ref_groups.items()):
                ref_alias = f"ref{ref_idx}"
                ref_alias_map[(owner, repo, ref)] = ref_alias
                for file_idx, (item_idx, path) in enumerate(files):
                    file_alias_map[item_idx] = (repo_idx, ref_alias, f"f{file_idx}")
            repo_idx += 1

        for list_pos, item_idx in enumerate(uncached_indices):
            owner, repo, ref, path = uncached_items[list_pos]
            cache_params = {"owner": owner, "repo": repo, "path": path}

            alias_info = file_alias_map.get(list_pos)
            if alias_info is None:
                results[item_idx] = HistoryResult(owner, repo, path, error="mapping_error")
                continue

            r_idx, ref_alias, f_alias = alias_info
            repo_alias = _make_alias(r_idx)
            repo_data = data.get(repo_alias)

            if repo_data is None:
                self.cache.set("file_history", cache_params, {"error": "not_found"})
                results[item_idx] = HistoryResult(owner, repo, path, error="not_found")
                continue

            ref_data = repo_data.get(ref_alias)
            if ref_data is None:
                self.cache.set("file_history", cache_params, {"error": "bad_ref"})
                results[item_idx] = HistoryResult(owner, repo, path, error="bad_ref")
                continue

            history_data = ref_data.get(f_alias)
            if history_data is None:
                self.cache.set("file_history", cache_params, {"error": "no_history"})
                results[item_idx] = HistoryResult(owner, repo, path, error="no_history")
                continue

            nodes = history_data.get("nodes", [])
            commits = []
            for node in nodes:
                commits.append({
                    "sha": node["oid"][:7],
                    "author": (node.get("author") or {}).get("name"),
                    "date": node.get("committedDate"),
                    "message": (node.get("messageHeadline") or "")[:80],
                })

            self.cache.set("file_history", cache_params, {"commits": commits})
            results[item_idx] = HistoryResult(owner, repo, path, commits=commits)

        return results

    def close(self):
        self._client.close()


def _build_metadata_query(repo_keys: list[str]) -> str:
    """Build a batched GraphQL query for repository metadata."""
    parts = []
    for i, repo_key in enumerate(repo_keys):
        owner, repo = repo_key.split("/", 1)
        owner_esc = _escape_graphql_string(owner)
        repo_esc = _escape_graphql_string(repo)
        parts.append(
            f'  {_make_alias(i)}: repository(owner: "{owner_esc}", name: "{repo_esc}") {{\n'
            f"    stargazerCount\n"
            f"    forkCount\n"
            f"    watchers {{ totalCount }}\n"
            f"    primaryLanguage {{ name }}\n"
            f"    repositoryTopics(first: 20) {{ nodes {{ topic {{ name }} }} }}\n"
            f"    createdAt\n"
            f"    updatedAt\n"
            f"    pushedAt\n"
            f"    defaultBranchRef {{ name }}\n"
            f"    licenseInfo {{ spdxId }}\n"
            f"    description\n"
            f"  }}"
        )
    return "query {\n" + "\n".join(parts) + "\n}"


def _build_history_query(items: list[tuple[str, str, str, str]]) -> str:
    """Build a batched GraphQL query for file commit history.

    Each item is (owner, repo, ref, path).
    Groups by (owner, repo) then by ref to minimize aliases.
    """
    # Group by (owner, repo) -> list of (index, ref, path)
    repo_groups: dict[tuple[str, str], list[tuple[int, str, str]]] = {}
    for i, (owner, repo, ref, path) in enumerate(items):
        key = (owner, repo)
        if key not in repo_groups:
            repo_groups[key] = []
        repo_groups[key].append((i, ref, path))

    parts = []
    for repo_idx, ((owner, repo), file_list) in enumerate(repo_groups.items()):
        # Group by ref within this repo
        ref_groups: dict[str, list[tuple[int, str]]] = {}
        for item_idx, ref, path in file_list:
            if ref not in ref_groups:
                ref_groups[ref] = []
            ref_groups[ref].append((item_idx, path))

        ref_parts = []
        for ref_idx, (ref, files) in enumerate(ref_groups.items()):
            file_parts = []
            for file_idx, (item_idx, path) in enumerate(files):
                path_esc = _escape_graphql_string(path)
                file_parts.append(
                    f'        f{file_idx}: history(first: 100, path: "{path_esc}") {{\n'
                    f"          nodes {{ oid messageHeadline committedDate author {{ name }} }}\n"
                    f"        }}"
                )
            files_block = "\n".join(file_parts)
            ref_esc = _escape_graphql_string(ref)
            ref_parts.append(
                f'    ref{ref_idx}: object(expression: "{ref_esc}") {{\n'
                f"      ... on Commit {{\n"
                f"{files_block}\n"
                f"      }}\n"
                f"    }}"
            )

        refs_block = "\n".join(ref_parts)
        owner_esc = _escape_graphql_string(owner)
        repo_esc = _escape_graphql_string(repo)
        parts.append(
            f'  {_make_alias(repo_idx)}: repository(owner: "{owner_esc}", name: "{repo_esc}") {{\n'
            f"{refs_block}\n"
            f"  }}"
        )

    return "query {\n" + "\n".join(parts) + "\n}"


def _parse_retry_after(resp: httpx.Response) -> float | None:
    """Parse Retry-After header if present."""
    val = resp.headers.get("retry-after")
    if val is None:
        return None
    try:
        return float(val)
    except ValueError:
        return None
