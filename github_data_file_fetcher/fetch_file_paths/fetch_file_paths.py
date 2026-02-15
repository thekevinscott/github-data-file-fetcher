"""v3 collection - linear scan with adaptive chunks."""

from pathlib import Path

from ..github import get_client
from ..models import GITHUB_SEARCH_RESULT_LIMIT
from ..db import get_file_count, get_scan_progress, init_db, insert_files, insert_search_hits, update_scan_progress
from .fetch_files import fetch_files

MAX_SIZE = 1_000_000  # 1MB

DEFAULT_CHUNK_SIZE = 10_000


def fetch_file_paths(
    query: str,
    db_path: Path | None = None,
    chunk: int = DEFAULT_CHUNK_SIZE,
    skip_cache: bool = False,
) -> None:
    """Fetch file paths via linear scan with adaptive chunk sizing."""
    init_db(db_path)
    client = get_client(skip_cache=skip_cache)

    # Check if scan already completed
    progress = get_scan_progress(db_path, query)
    if progress and progress["completed_at"] and not skip_cache:
        collected = get_file_count(db_path)
        print(f"Scan already completed ({collected:,} files). Use --skip-cache to rescan.", flush=True)
        return

    # Get total to know when we're done
    total = client.search_code(query, per_page=1, page=1).get("total_count", 0)
    print(f"Total: {total:,}", flush=True)

    # Resume from existing progress
    collected = get_file_count(db_path)
    if collected > 0:
        print(f"Resuming: {collected:,} already collected", flush=True)

    # Resume from last scan position
    lo = 0
    if progress and progress["last_lo"] > 0 and not skip_cache:
        lo = progress["last_lo"]
        print(f"Resuming scan from size:{lo}", flush=True)

    consecutive_empty = 0
    MAX_CONSECUTIVE_EMPTY = 10

    while lo < MAX_SIZE:
        hi = min(lo + chunk, MAX_SIZE)
        count = client.search_code(f"{query} size:{lo}..{hi}", per_page=1, page=1).get(
            "total_count", 0
        )

        if count == 0:
            # Empty region - widen and advance
            print(f"  size:{lo}..{hi} = 0 (skipping)", flush=True)
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                print(f"  {consecutive_empty} consecutive empty ranges, stopping", flush=True)
                break
            lo = hi + 1
            chunk = min(chunk * 2, MAX_SIZE)

        elif count < GITHUB_SEARCH_RESULT_LIMIT:
            # Collectible - grab it, widen, advance
            consecutive_empty = 0
            print(f"  size:{lo}..{hi} = {count:,} (collecting)", flush=True)
            full_query = f"{query} size:{lo}..{hi}"
            files = fetch_files(full_query)

            # Track every hit for analysis
            hits = [
                {"url": f["html_url"], "query": full_query, "size_min": lo, "size_max": hi}
                for f in files
            ]
            insert_search_hits(db_path, hits)

            if len(files) >= GITHUB_SEARCH_RESULT_LIMIT:
                # GitHub lied about count - narrow and retry
                print(f"    ^ hit ceiling ({len(files)}), narrowing", flush=True)
                chunk = max(chunk // 2, 1)
            else:
                new_count = insert_files(db_path, files)
                collected += new_count
                print(f"    ^ stored {new_count} new ({collected:,} total)", flush=True)
                lo = hi + 1
                chunk = min(chunk * 2, MAX_SIZE)

        else:
            # Too dense - narrow, stay put
            consecutive_empty = 0
            print(f"  size:{lo}..{hi} = {count:,} (narrowing)", flush=True)
            chunk = max(chunk // 2, 1)

        # Save progress periodically
        update_scan_progress(db_path, query, lo, MAX_SIZE, collected)

    # Mark scan as completed
    update_scan_progress(db_path, query, lo, MAX_SIZE, collected, completed=True)
    print(f"\nDone. Collected {collected} / {total:,}", flush=True)
