"""Collect files from GitHub using linear scanning.

Uses linear scanning across file sizes with adaptive chunk sizing to work
around GitHub's 1000-result search limit.
"""

from .cli import main
from .generic_client import get_generic_client
from .models import ApiResponse

__all__ = ["main", "get_generic_client", "ApiResponse"]

if __name__ == "__main__":
    main()
