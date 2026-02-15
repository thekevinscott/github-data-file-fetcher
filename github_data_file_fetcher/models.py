"""Data models and constants for skill collection."""

from dataclasses import dataclass

GITHUB_SEARCH_RESULT_LIMIT = 1000  # GitHub Code Search API hard limit per query
MAX_FILE_CONTENT_LENGTH = 10_000  # Truncate files longer than this for classification


@dataclass
class ApiResponse:
    """Response from the generic GitHub REST API client."""

    status: int
    body: dict | list
    etag: str | None = None
    link: str | None = None
