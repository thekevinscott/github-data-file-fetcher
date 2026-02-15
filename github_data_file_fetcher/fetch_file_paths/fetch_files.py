from ..github import get_client

MAX_EMPTY_RETRIES = 3


def fetch_files(query: str) -> list[dict]:
    """Fetch all results for a query (up to 1000).

    Handles rate limits by retrying empty responses up to MAX_EMPTY_RETRIES times.
    """
    client = get_client()
    collected = []
    page = 1
    empty_retries = 0
    expected_total = None

    # page cannot be greater than 10 (GitHub limit: 10 pages * 100 = 1000 results)
    while page <= 10:
        result = client.search_code(query, per_page=100, page=page)
        items = result.get("items", [])

        # Track expected total from first response
        if expected_total is None:
            expected_total = result.get("total_count", 0)

        if not items:
            # Check if we've collected enough (accounting for GitHub's 1000 limit)
            expected_so_far = min(expected_total, page * 100)
            if len(collected) >= expected_so_far or len(collected) >= expected_total:
                # We've collected what we expected, this is a real end
                break

            # Empty response but we expected more - likely rate limit issue
            empty_retries += 1
            if empty_retries >= MAX_EMPTY_RETRIES:
                print(
                    f"    [WARN] Got {empty_retries} empty responses, giving up on remaining pages",
                    flush=True,
                )
                break

            print(
                f"    [RETRY] Empty response on page {page}, expected ~{expected_total} total, have {len(collected)}. Retry {empty_retries}/{MAX_EMPTY_RETRIES}",
                flush=True,
            )
            # Don't increment page - retry the same page
            continue

        # Got items - reset empty retry counter and continue
        empty_retries = 0
        collected.extend(items)
        page += 1

    return collected
