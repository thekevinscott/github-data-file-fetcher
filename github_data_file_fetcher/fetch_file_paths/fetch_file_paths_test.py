"""Unit tests for fetch_file_paths adaptive chunking logic."""

from unittest.mock import MagicMock, patch

from .fetch_file_paths import MAX_SIZE, fetch_file_paths


def _make_files(n):
    return [{"html_url": f"https://github.com/o/r/blob/main/f{i}", "sha": f"sha{i}"} for i in range(n)]


def _run(search_counts, fetch_returns, chunk=10_000):
    """Run fetch_file_paths with mocked search counts and fetch results.

    search_counts: list of total_count values returned by search_code, in order.
                   First call is the initial total query; rest are range queries.
    fetch_returns: list of file lists returned by fetch_files, in order.

    Returns (search_calls, fetch_calls) — the actual call args for inspection.
    """
    search_idx = [0]
    fetch_idx = [0]

    def search_side_effect(query, per_page=100, page=1):
        i = search_idx[0]
        search_idx[0] += 1
        count = search_counts[i] if i < len(search_counts) else 0
        return {"total_count": count}

    def fetch_side_effect(query):
        i = fetch_idx[0]
        fetch_idx[0] += 1
        return fetch_returns[i] if i < len(fetch_returns) else []

    client = MagicMock()
    client.search_code.side_effect = search_side_effect

    with (
        patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.get_client", return_value=client),
        patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.fetch_files", side_effect=fetch_side_effect),
        patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.init_db"),
        patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.get_scan_progress", return_value=None),
        patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.get_file_count", return_value=0),
        patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.insert_files", side_effect=lambda _db, files: len(files)),
        patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.insert_search_hits"),
        patch("github_data_file_fetcher.fetch_file_paths.fetch_file_paths.update_scan_progress"),
    ):
        fetch_file_paths("filename:SKILL.md", db_path=None, chunk=chunk)

    return client.search_code.call_args_list


def _range_from_call(call):
    """Extract (lo, hi) from a search_code call like 'filename:SKILL.md size:100..200'."""
    query = call[0][0]
    if "size:" not in query:
        return None
    part = query.split("size:")[1]
    lo, hi = part.split("..")
    return int(lo), int(hi)


def describe_adaptive_chunking():

    def it_collects_small_range_and_advances():
        """A range under the limit is collected and lo advances past hi."""
        search_counts = [
            50,   # total query
            50,   # size:0..9999 = 50
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  # empty ranges until stop
        ]
        fetch_returns = [_make_files(50)]

        calls = _run(search_counts, fetch_returns)
        # First range query should be size:0..9999
        assert _range_from_call(calls[1]) == (0, 9999)
        # Next range query should start at 10000
        assert _range_from_call(calls[2])[0] == 10000

    def it_narrows_when_count_exceeds_limit():
        """A range with count >= 1000 triggers narrowing (halves chunk)."""
        search_counts = [
            2000,   # total query
            2000,   # size:0..9999 = 2000 (too dense, narrow)
            500,    # size:0..4999 = 500 (collectible)
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  # empty until stop
        ]
        fetch_returns = [_make_files(500)]

        calls = _run(search_counts, fetch_returns)
        # First range: 0..9999 (chunk=10000)
        assert _range_from_call(calls[1]) == (0, 9999)
        # Narrowed: 0..4999 (chunk=5000)
        assert _range_from_call(calls[2]) == (0, 4999)

    def it_narrows_to_exact_size_when_fetch_hits_ceiling():
        """When fetch returns >= 1000 despite count < 1000, chunk narrows to 1 (exact size)."""
        search_counts = [
            500,   # total
            500,   # size:0..1 (chunk=2) = 500 (collectible)
            # ^ but fetch returns 1000, so narrow chunk 2->1
            250,   # size:0..0 (chunk=1, exact) = 250
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
        fetch_returns = [
            _make_files(1000),  # first fetch hits ceiling
            _make_files(250),   # second fetch succeeds
        ]

        calls = _run(search_counts, fetch_returns, chunk=2)
        # First range: 0..1 (chunk=2)
        assert _range_from_call(calls[1]) == (0, 1)
        # Narrowed to exact: 0..0 (chunk=1)
        assert _range_from_call(calls[2]) == (0, 0)

    def it_collects_and_advances_at_exact_size_ceiling():
        """When even an exact size query (lo==hi) returns >= 1000, collect and move on."""
        search_counts = [
            2000,   # total
            500,    # size:0..0 (chunk=1) = 500 (collectible)
            # ^ but fetch returns 1000 and lo==hi, so collect and advance
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
        fetch_returns = [
            _make_files(1000),  # exact size still hits ceiling
        ]

        calls = _run(search_counts, fetch_returns, chunk=1)
        # First range: exact 0..0
        assert _range_from_call(calls[1]) == (0, 0)
        # Must advance past 0, not loop forever
        next_range = _range_from_call(calls[2])
        assert next_range[0] == 1

    def it_does_not_infinite_loop_on_ceiling_at_chunk_2():
        """Regression: chunk=2 range hitting ceiling narrows to chunk=1, not stuck."""
        search_counts = [
            1500,   # total
            630,    # size:0..1 (chunk=2) = 630 (collectible)
            # ^ but fetch returns 1000, so narrow chunk 2->1
            242,    # size:0..0 (chunk=1, exact) = 242 (collectible, fetch OK)
            # ^ after collect, chunk widens back to 2
            306,    # size:1..2 (chunk=2) = 306 (collectible, fetch OK)
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
        fetch_returns = [
            _make_files(1000),  # 0..1 hits ceiling
            _make_files(242),   # 0..0 OK
            _make_files(306),   # 1..2 OK
        ]

        calls = _run(search_counts, fetch_returns, chunk=2)
        ranges = [_range_from_call(c) for c in calls if _range_from_call(c)]
        # Should narrow from 0..1 to 0..0, then widen and advance — no repeats
        assert ranges[0] == (0, 1)
        assert ranges[1] == (0, 0)
        assert ranges[2] == (1, 2)
        # Verify no repeated ranges (the original bug)
        assert len(ranges) == len(set(ranges))

    def it_widens_chunk_after_collecting():
        """After a successful collect, chunk doubles for the next range."""
        search_counts = [
            100,   # total
            50,    # size:0..4 (chunk=5) = 50
            50,    # size:5..14 (chunk=10, doubled) = 50
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
        fetch_returns = [_make_files(50), _make_files(50)]

        calls = _run(search_counts, fetch_returns, chunk=5)
        assert _range_from_call(calls[1]) == (0, 4)
        # chunk doubled: 5 -> 10, so next is 5..14
        assert _range_from_call(calls[2]) == (5, 14)

    def it_stops_after_consecutive_empty_ranges():
        """10 consecutive empty ranges triggers early stop."""
        # chunk=1 prevents doubling past MAX_SIZE so we actually get 10 empties
        search_counts = [100] + [0] * 15

        calls = _run(search_counts, [], chunk=1)
        range_calls = [c for c in calls if _range_from_call(c)]
        assert len(range_calls) == 10
