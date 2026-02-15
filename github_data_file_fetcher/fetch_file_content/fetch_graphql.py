"""Fetch file content using GitHub GraphQL API with batching."""

import base64
import os
import sys
import threading
from pathlib import Path

from ..github import get_client
from ..graphql import GraphQLClient
from ..utils import parse_github_url, resolve_content_path
from .fetch_file_content import _scan_existing

DEFAULT_BATCH_SIZE = 50


def fetch_file_content_graphql(
    urls: list[str], content_dir: Path, db_path: Path | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> dict:
    """Fetch content for GitHub URLs using batched GraphQL queries.

    URLs should be pre-filtered to only include those needing content.
    Records content status in DB so re-runs skip already-processed files.
    Falls back to REST for truncated blobs (>100KB).
    Returns dict with counts: fetched, skipped, not_found, errors, truncated, queries.
    """
    content_dir.mkdir(parents=True, exist_ok=True)
    gql = GraphQLClient()
    rest_client = None  # lazy-init only if needed

    stats = {
        "fetched": 0,
        "skipped": 0,
        "not_found": 0,
        "errors": 0,
        "truncated_rest": 0,
        "queries": 0,
    }
    total = len(urls)

    # Phase 1: Pre-scan existing files in bulk to skip items already on disk
    # (handles files fetched before content_status tracking was added)
    existing = _scan_existing(content_dir)
    pending: list[tuple[str, str, str, str, str]] = []  # (url, owner, repo, ref, path)
    already_on_disk: list[tuple[str, str]] = []
    for url in urls:
        parsed = parse_github_url(url)
        if not parsed:
            stats["errors"] += 1
            continue
        owner, repo, ref, path = parsed
        rel = f"{owner}/{repo}/blob/{ref}/{path}"
        if rel in existing:
            stats["skipped"] += 1
            already_on_disk.append((url, "fetched"))
        else:
            pending.append((url, owner, repo, ref, path))

    # Record already-on-disk items in content_status so future runs skip them via DB
    if db_path and already_on_disk:
        from ..db import insert_content_status_batch
        insert_content_status_batch(db_path, already_on_disk)

    # Progress display
    done_event = threading.Event()

    def print_progress():
        done = stats["fetched"] + stats["skipped"] + stats["not_found"] + stats["errors"] + stats["truncated_rest"]
        cache_hits = gql.cache.hits
        queries = gql.queries
        avg_ms = gql.avg_query_time * 1000
        qps = gql.queries_per_sec
        rl = gql.rate_limit_hits
        retries = gql.retries
        timing = f"{avg_ms:.0f}ms/q, {qps:.1f}q/s"
        issues = f", {rl} rate limits, {retries} retries" if rl or retries else ""
        line = (
            f"  [{done}/{total}] {stats['skipped']} skip, {stats['fetched']} fetched, "
            f"{stats['not_found']} 404, {stats['errors']} err, "
            f"{cache_hits} cached, {queries}q ({timing}{issues})"
        )
        sys.stdout.write(f"\033[2K\r{line}")
        sys.stdout.flush()

    def refresh_display():
        while not done_event.is_set():
            print_progress()
            done_event.wait(0.01)

    refresh_thread = threading.Thread(target=refresh_display, daemon=True)
    refresh_thread.start()

    # Collect status records for DB
    status_records: list[tuple[str, str]] = []

    # Phase 2: Batch GraphQL fetches
    truncated: list[tuple[str, str, str, str, str]] = []  # (url, owner, repo, ref, path) needing REST fallback

    try:
        for batch_start in range(0, len(pending), batch_size):
            batch = pending[batch_start : batch_start + batch_size]
            items = [(owner, repo, ref, path) for _, owner, repo, ref, path in batch]

            results = gql.fetch_batch(items)

            for (url, owner, repo, ref, path), result in zip(batch, results):
                if result is None:
                    stats["errors"] += 1
                    status_records.append((url, "error"))
                    continue

                if result.error == "truncated":
                    truncated.append((url, result.owner, result.repo, result.ref, result.path))
                    continue

                if result.error in ("not_found", "unresolvable_symlink"):
                    stats["not_found"] += 1
                    status_records.append((url, "not_found"))
                    continue

                if result.error:
                    stats["errors"] += 1
                    status_records.append((url, "error"))
                    continue

                if result.content_b64:
                    content = base64.b64decode(result.content_b64).decode("utf-8", errors="replace")
                    local_path = resolve_content_path(
                        content_dir, result.owner, result.repo, result.ref, result.path
                    )
                    try:
                        local_path.parent.mkdir(parents=True, exist_ok=True)
                        local_path.write_text(content)
                    except OSError:
                        stats["errors"] += 1
                        status_records.append((url, "error"))
                        continue
                    stats["fetched"] += 1
                    status_records.append((url, "fetched"))
                else:
                    stats["errors"] += 1
                    status_records.append((url, "error"))

        # Phase 3: REST fallback for truncated blobs
        if truncated:
            rest_client = get_client()
            for url, owner, repo, ref, path in truncated:
                try:
                    data = rest_client.get_file_content(owner, repo, path, ref=ref)
                    if data.get("content") is not None:
                        content = base64.b64decode(data["content"]).decode(
                            "utf-8", errors="replace"
                        )
                        local_path = resolve_content_path(content_dir, owner, repo, ref, path)
                        try:
                            local_path.parent.mkdir(parents=True, exist_ok=True)
                            local_path.write_text(content)
                        except OSError:
                            stats["errors"] += 1
                            status_records.append((url, "error"))
                            continue
                        stats["truncated_rest"] += 1
                        status_records.append((url, "fetched"))
                    else:
                        stats["not_found"] += 1
                        status_records.append((url, "not_found"))
                except FileNotFoundError:
                    stats["not_found"] += 1
                    status_records.append((url, "not_found"))
                except Exception:
                    stats["errors"] += 1
                    status_records.append((url, "error"))

    except KeyboardInterrupt:
        pass

    done_event.set()
    stats["queries"] = gql.queries
    gql.close()

    # Record content status in DB
    if db_path and status_records:
        from ..db import insert_content_status_batch
        insert_content_status_batch(db_path, status_records)

    sys.stdout.write("\n")
    sys.stdout.flush()
    return stats
