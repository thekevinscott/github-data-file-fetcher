"""Unit tests for fetch_files pagination logic."""

from unittest.mock import MagicMock, patch

from .fetch_files import MAX_EMPTY_RETRIES, fetch_files


def describe_fetch_files():

    def _mock_client(pages):
        """Create a mock client that returns pages as a dict of page_num -> items.

        pages: dict mapping page number (1-indexed) to list of item dicts.
        Missing pages return empty.
        """
        client = MagicMock()
        total = sum(len(v) for v in pages.values())

        def search_side_effect(query, per_page=100, page=1):
            items = pages.get(page, [])
            return {"total_count": total, "items": items}

        client.search_code.side_effect = search_side_effect
        return client

    def it_collects_all_pages_up_to_10():
        # 3 pages of 100 items each
        pages = {p: [{"html_url": f"u{i}", "sha": f"s{i}"} for i in range((p - 1) * 100, p * 100)] for p in range(1, 4)}
        client = _mock_client(pages)

        with patch("github_data_file_fetcher.fetch_file_paths.fetch_files.get_client", return_value=client):
            result = fetch_files("test query")

        assert len(result) == 300

    def it_stops_when_collected_matches_expected(self=None):
        """Stops when we've collected the expected total."""
        pages = {1: [{"html_url": f"u{i}", "sha": f"s{i}"} for i in range(50)]}
        client = _mock_client(pages)

        with patch("github_data_file_fetcher.fetch_file_paths.fetch_files.get_client", return_value=client):
            result = fetch_files("test query")

        assert len(result) == 50
        # Should only call page 1, then page 2 (empty, triggers stop since collected >= expected)
        assert client.search_code.call_count == 2

    def it_retries_empty_pages_up_to_max(self=None):
        """Empty response when more expected triggers retry, gives up after MAX_EMPTY_RETRIES."""
        # Page 1 returns items, page 2 always empty despite expecting more
        pages = {1: [{"html_url": f"u{i}", "sha": f"s{i}"} for i in range(100)]}
        client = MagicMock()
        call_count = [0]

        def search_side_effect(query, per_page=100, page=1):
            call_count[0] += 1
            items = pages.get(page, [])
            return {"total_count": 200, "items": items}

        client.search_code.side_effect = search_side_effect

        with patch("github_data_file_fetcher.fetch_file_paths.fetch_files.get_client", return_value=client):
            result = fetch_files("test query")

        assert len(result) == 100
        # Page 1 (1 call) + page 2 (MAX_EMPTY_RETRIES retries)
        assert client.search_code.call_count == 1 + MAX_EMPTY_RETRIES

    def it_handles_single_page_result():
        pages = {1: [{"html_url": "u1", "sha": "s1"}]}
        client = _mock_client(pages)

        with patch("github_data_file_fetcher.fetch_file_paths.fetch_files.get_client", return_value=client):
            result = fetch_files("test query")

        assert len(result) == 1

    def it_handles_zero_results():
        pages = {1: []}
        client = MagicMock()
        client.search_code.return_value = {"total_count": 0, "items": []}

        with patch("github_data_file_fetcher.fetch_file_paths.fetch_files.get_client", return_value=client):
            result = fetch_files("test query")

        assert len(result) == 0
