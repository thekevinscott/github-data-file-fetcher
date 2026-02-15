"""Fetch repository metadata from GitHub."""

import sys
import time
from pathlib import Path

from github import GithubException, RateLimitExceededException

from ..db import get_repos_without_metadata, get_unique_repos, init_db, insert_repo_metadata
from ..github import get_client

# Cache endpoint key for repo metadata
_CACHE_ENDPOINT = "repo_metadata"


def _progress(msg: str):
    sys.stdout.write(f"\033[2K\r{msg}")
    sys.stdout.flush()


def _log(msg: str):
    sys.stderr.write(f"\033[2K\r[metadata] {msg}\n")
    sys.stderr.flush()


def _wait_for_rate_limit(client, e):
    """Handle rate limit with visible countdown."""
    client._rate_limit_hits += 1
    reset_time = client.github.rate_limiting_resettime
    client._rate_limit_reset = max(client._rate_limit_reset, reset_time + 1)
    while time.time() < client._rate_limit_reset:
        remaining = int(client._rate_limit_reset - time.time())
        _progress(f"  Rate limited, waiting {remaining}s...")
        time.sleep(1)


def fetch_repo_metadata(db_path: Path | None = None) -> dict:
    """Fetch metadata for repos that don't have it yet.

    Returns dict with counts: fetched, errors.
    """
    init_db(db_path)
    client = get_client()

    all_repos = get_unique_repos(db_path)
    repos = get_repos_without_metadata(db_path)

    print(f"Found {len(all_repos)} unique repos, {len(repos)} need metadata", flush=True)

    cache = client.cache
    stats = {"fetched": 0, "errors": 0, "cache_hits": 0}

    for _i, repo_key in enumerate(repos):
        owner, repo = repo_key.split("/", 1)
        cache_params = {"repo_key": repo_key}

        # Check cache first
        cached = cache.get(_CACHE_ENDPOINT, cache_params)
        if cached is not None:
            if cached.get("error"):
                stats["errors"] += 1
            else:
                insert_repo_metadata(db_path, repo_key, cached)
                stats["fetched"] += 1
            stats["cache_hits"] += 1
            total = stats["fetched"] + stats["errors"]
            _progress(
                f"  [{total}/{len(repos)}] {stats['fetched']} fetched, "
                f"{stats['errors']} errors, {stats['cache_hits']} cached"
            )
            continue

        for _attempt in range(10):
            try:
                client._throttle()
                repo_obj = client.github.get_repo(repo_key)
                metadata = {
                    "stars": repo_obj.stargazers_count,
                    "forks": repo_obj.forks_count,
                    "watchers": repo_obj.watchers_count,
                    "language": repo_obj.language,
                    "topics": repo_obj.get_topics(),
                    "created_at": repo_obj.created_at.isoformat() if repo_obj.created_at else None,
                    "updated_at": repo_obj.updated_at.isoformat() if repo_obj.updated_at else None,
                    "pushed_at": repo_obj.pushed_at.isoformat() if repo_obj.pushed_at else None,
                    "default_branch": repo_obj.default_branch,
                    "license": repo_obj.license.spdx_id if repo_obj.license else None,
                    "description": repo_obj.description,
                }
                cache.set(_CACHE_ENDPOINT, cache_params, metadata)
                insert_repo_metadata(db_path, repo_key, metadata)
                stats["fetched"] += 1
                break
            except RateLimitExceededException as e:
                _wait_for_rate_limit(client, e)
            except GithubException as e:
                if e.status in (403, 429) and "rate limit" in str(e).lower():
                    _wait_for_rate_limit(
                        client, RateLimitExceededException(e.status, e.data, e.headers)
                    )
                elif e.status == 404:
                    cache.set(_CACHE_ENDPOINT, cache_params, {"error": "not_found"})
                    stats["errors"] += 1
                    break
                else:
                    _log(f"Error: {e}")
                    stats["errors"] += 1
                    break
            except Exception as e:
                _log(f"Error: {e}")
                stats["errors"] += 1
                break

        # Progress
        total = stats["fetched"] + stats["errors"]
        rate = f", rate limited ({client.rate_limit_waiting}s)" if client.rate_limit_waiting else ""
        _progress(
            f"  [{total}/{len(repos)}] {stats['fetched']} fetched, "
            f"{stats['errors']} errors, {stats['cache_hits']} cached{rate}"
        )

    sys.stdout.write("\n")
    sys.stdout.flush()
    return stats
