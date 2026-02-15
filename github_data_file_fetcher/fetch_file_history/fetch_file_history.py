"""Fetch commit history for skill files."""

import sys
import time
from pathlib import Path

from github import GithubException, RateLimitExceededException

from ..db import get_files_without_history, init_db, insert_file_history
from ..github import get_client
from ..utils import parse_github_url

# Cache endpoint key for file history
_CACHE_ENDPOINT = "file_history"


def _progress(msg: str):
    sys.stdout.write(f"\033[2K\r{msg}")
    sys.stdout.flush()


def _log(msg: str):
    sys.stderr.write(f"\033[2K\r[history] {msg}\n")
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


def fetch_file_history(db_path: Path | None = None) -> dict:
    """Fetch commit history for files that don't have it yet.

    Returns dict with counts: fetched, errors.
    """
    init_db(db_path)
    client = get_client()

    urls = get_files_without_history(db_path)

    print(f"Found {len(urls)} files without history", flush=True)

    cache = client.cache
    stats = {"fetched": 0, "errors": 0, "cache_hits": 0}

    for _i, url in enumerate(urls):
        parsed = parse_github_url(url)
        if not parsed:
            stats["errors"] += 1
            continue

        owner, repo, _, path = parsed
        cache_params = {"owner": owner, "repo": repo, "path": path}

        # Check cache first
        cached = cache.get(_CACHE_ENDPOINT, cache_params)
        if cached is not None:
            if cached.get("error"):
                stats["errors"] += 1
            else:
                insert_file_history(db_path, url, cached.get("commits", []))
                stats["fetched"] += 1
            stats["cache_hits"] += 1
            total = stats["fetched"] + stats["errors"]
            _progress(
                f"  [{total}/{len(urls)}] {stats['fetched']} fetched, "
                f"{stats['errors']} errors, {stats['cache_hits']} cached"
            )
            continue

        for _attempt in range(10):
            try:
                client._throttle()
                repo_obj = client.github.get_repo(f"{owner}/{repo}")
                commits_iter = repo_obj.get_commits(path=path)

                # Get up to 100 commits
                commit_list = []
                for j, commit in enumerate(commits_iter):
                    if j >= 100:
                        break
                    commit_list.append(
                        {
                            "sha": commit.sha[:7],
                            "author": commit.commit.author.name if commit.commit.author else None,
                            "date": commit.commit.author.date.isoformat()
                            if commit.commit.author and commit.commit.author.date
                            else None,
                            "message": commit.commit.message.split("\n")[0][:80],
                        }
                    )

                cache.set(_CACHE_ENDPOINT, cache_params, {"commits": commit_list})
                insert_file_history(db_path, url, commit_list)
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
            f"  [{total}/{len(urls)}] {stats['fetched']} fetched, "
            f"{stats['errors']} errors, {stats['cache_hits']} cached{rate}"
        )

    sys.stdout.write("\n")
    sys.stdout.flush()
    return stats
