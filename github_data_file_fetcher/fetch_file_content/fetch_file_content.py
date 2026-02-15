"""Fetch file content from GitHub URLs."""

import base64
import logging
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

logging.getLogger("github").setLevel(logging.ERROR)
logging.getLogger("github.Requester").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

from ..github import get_client
from ..utils import parse_github_url, resolve_content_path

MAX_WORKERS = 10


def _scan_existing(content_dir: Path) -> set[str]:
    """Pre-scan content directory, returning set of relative paths.

    Uses os.walk (bulk directory reads) instead of per-file stat() calls.
    At 190K+ files, this is ~100x faster than individual Path.exists() checks.
    """
    existing = set()
    if not content_dir.exists():
        return existing
    base_len = len(str(content_dir)) + 1  # +1 for separator
    for dirpath, _, filenames in os.walk(content_dir):
        for name in filenames:
            existing.add(os.path.join(dirpath, name)[base_len:])
    return existing


def fetch_file_content(urls: list[str], content_dir: Path, db_path: Path | None = None) -> dict:
    """Fetch content for a list of GitHub URLs.

    URLs should be pre-filtered to only include those needing content.
    Records content status in DB so re-runs skip already-processed files.
    Returns dict with counts: fetched, skipped, not_found, errors.
    """
    content_dir.mkdir(parents=True, exist_ok=True)
    client = get_client()

    stats = {"fetched": 0, "skipped": 0, "not_found": 0, "errors": 0}

    # Pre-scan existing files in bulk to skip items already on disk
    # (handles files fetched before content_status tracking was added)
    existing = _scan_existing(content_dir)
    pending: list[tuple[str, str, str, str, str]] = []  # (url, owner, repo, ref, path)
    already_on_disk: list[tuple[str, str]] = []  # (url, status) for DB recording
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

    total = len(urls)

    def print_progress():
        done = stats["fetched"] + stats["skipped"] + stats["not_found"] + stats["errors"]
        retries = f", {client.api_retries} retries" if client.api_retries else ""
        rate = f", rate limited ({client.rate_limit_waiting}s)" if client.rate_limit_waiting else ""
        line = f"  [{done}/{total}] {stats['skipped']} skipped, {stats['fetched']} fetched, {stats['not_found']} not found, {stats['errors']} errors, {client.cache.hits} cache hits{retries}{rate}"
        sys.stdout.write(f"\033[2K\r{line}")
        sys.stdout.flush()

    if not pending:
        print_progress()
        sys.stdout.write("\n")
        sys.stdout.flush()
        return stats

    # Track status for DB recording
    status_lock = threading.Lock()
    status_records: list[tuple[str, str]] = []

    def process_one(item):
        url, owner, repo, ref, path = item
        local_path = resolve_content_path(content_dir, owner, repo, ref, path)
        status = "error"

        try:
            data = client.get_file_content(owner, repo, path, ref=ref)
            if data.get("content") is not None:
                content = base64.b64decode(data["content"]).decode("utf-8", errors="replace")
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_text(content)
                stats["fetched"] += 1
                status = "fetched"
            else:
                stats["not_found"] += 1
                status = "not_found"
        except FileNotFoundError:
            stats["not_found"] += 1
            status = "not_found"
        except Exception:
            stats["errors"] += 1

        with status_lock:
            status_records.append((url, status))

    # Background thread to refresh display during rate limit waits
    done_event = threading.Event()

    def refresh_display():
        while not done_event.is_set():
            print_progress()
            done_event.wait(0.01)

    refresh_thread = threading.Thread(target=refresh_display, daemon=True)
    refresh_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            list(executor.map(process_one, pending))
    except KeyboardInterrupt:
        pass

    done_event.set()

    # Record content status in DB
    if db_path and status_records:
        from ..db import insert_content_status_batch
        insert_content_status_batch(db_path, status_records)

    sys.stdout.write("\n")
    sys.stdout.flush()
    return stats
