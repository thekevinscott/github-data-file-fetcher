"""Fetch file commit history using GitHub GraphQL API with batching."""

import sys
from pathlib import Path

from ..db import get_files_without_history, init_db, insert_file_history_batch
from ..graphql import GraphQLClient
from ..utils import parse_github_url

DEFAULT_BATCH_SIZE = 20


def _progress(msg: str):
    sys.stdout.write(f"\033[2K\r{msg}")
    sys.stdout.flush()


def fetch_file_history_graphql(db_path: Path, batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
    """Fetch commit history for files that don't have it yet, using GraphQL batching.

    Returns dict with counts: fetched, errors, queries.
    """
    init_db(db_path)
    gql = GraphQLClient()

    urls = get_files_without_history(db_path)

    print(f"Found {len(urls)} files without history", flush=True)

    # Parse all URLs upfront, track url->parsed mapping
    parsed_items: list[tuple[str, tuple[str, str, str, str] | None]] = []
    for url in urls:
        parsed_items.append((url, parse_github_url(url)))

    stats = {"fetched": 0, "errors": 0, "cache_hits": 0}

    # Build list of (url, owner, repo, ref, path) for valid URLs
    valid: list[tuple[str, str, str, str, str]] = []
    for url, parsed in parsed_items:
        if parsed is None:
            stats["errors"] += 1
        else:
            owner, repo, ref, path = parsed
            valid.append((url, owner, repo, ref, path))

    total_items = len(valid) + stats["errors"]

    try:
        for batch_start in range(0, len(valid), batch_size):
            batch = valid[batch_start : batch_start + batch_size]
            items = [(owner, repo, ref, path) for _, owner, repo, ref, path in batch]
            batch_urls = [url for url, _, _, _, _ in batch]

            results = gql.fetch_history_batch(items)

            db_batch = []
            for url, result in zip(batch_urls, results):
                if result is None:
                    stats["errors"] += 1
                    continue

                if result.error:
                    stats["errors"] += 1
                    continue

                if result.commits is not None:
                    db_batch.append((url, result.commits))
                    stats["fetched"] += 1

            insert_file_history_batch(db_path, db_batch)

            total = stats["fetched"] + stats["errors"]
            cache_hits = gql.cache.hits
            queries = gql.queries
            avg_ms = gql.avg_query_time * 1000
            qps = gql.queries_per_sec
            _progress(
                f"  [{total}/{total_items}] {stats['fetched']} fetched, "
                f"{stats['errors']} errors, {cache_hits} cached, "
                f"{queries}q ({avg_ms:.0f}ms/q, {qps:.1f}q/s)"
            )

    except KeyboardInterrupt:
        pass

    stats["queries"] = gql.queries
    gql.close()

    sys.stdout.write("\n")
    sys.stdout.flush()
    return stats
