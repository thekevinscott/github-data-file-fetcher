"""Fetch repository metadata using GitHub GraphQL API with batching."""

import sys
from pathlib import Path

from ..db import get_repos_without_metadata, get_unique_repos, init_db, insert_repo_metadata_batch
from ..graphql import GraphQLClient

DEFAULT_BATCH_SIZE = 50


def _progress(msg: str):
    sys.stdout.write(f"\033[2K\r{msg}")
    sys.stdout.flush()


def fetch_repo_metadata_graphql(db_path: Path, batch_size: int = DEFAULT_BATCH_SIZE) -> dict:
    """Fetch metadata for repos that don't have it yet, using GraphQL batching.

    Returns dict with counts: fetched, errors, queries.
    """
    init_db(db_path)
    gql = GraphQLClient()

    all_repos = get_unique_repos(db_path)
    repos = get_repos_without_metadata(db_path)

    print(f"Found {len(all_repos)} unique repos, {len(repos)} need metadata", flush=True)

    stats = {"fetched": 0, "errors": 0, "cache_hits": 0}

    try:
        for batch_start in range(0, len(repos), batch_size):
            batch = repos[batch_start : batch_start + batch_size]
            results = gql.fetch_metadata_batch(batch)

            db_batch = []
            for result in results:
                if result is None:
                    stats["errors"] += 1
                    continue

                if result.error:
                    stats["errors"] += 1
                    continue

                if result.metadata:
                    db_batch.append((result.repo_key, result.metadata))
                    stats["fetched"] += 1

            insert_repo_metadata_batch(db_path, db_batch)

            total = stats["fetched"] + stats["errors"]
            cache_hits = gql.cache.hits
            queries = gql.queries
            avg_ms = gql.avg_query_time * 1000
            qps = gql.queries_per_sec
            _progress(
                f"  [{total}/{len(repos)}] {stats['fetched']} fetched, "
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
