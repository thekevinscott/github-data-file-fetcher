# Architecture

## Project Structure

```
github_data_file_fetcher/
  __init__.py              # Entry point, public exports
  cli.py                   # CLI commands (5 subcommands)
  db.py                    # SQLite schema and connection helpers
  models.py                # ApiResponse dataclass, constants
  generic_client.py        # Generic REST client (httpx + Cachetta)
  github.py                # PyGithub-based client + Cache class
  graphql.py               # GraphQL batch client + generic graphql()
  utils.py                 # URL parsing, text formatting
  settings.py              # GITHUB_TOKEN config via pydantic-settings

  fetch_file_paths/        # Linear scan collection
  fetch_file_content/      # Content fetching (REST + GraphQL)
  fetch_repo_metadata/     # Repository metadata (REST + GraphQL)
  fetch_file_history/      # Commit history (REST + GraphQL)
```

## Testing

```bash
# All tests
uv run pytest

# Just the generic API tests
uv run pytest tests/integration/test_generic_api.py -v

# With coverage
uv run pytest --cov=github_data_file_fetcher
```

Tests use real SQLite databases and real file-based caches in temp directories. External API calls are mocked.

## Known Limitations

- GitHub code search returns a maximum of 1,000 results per query. The linear scan shards by file size to work around this, but if a single byte-range has >1,000 files, an exception is raised.
- The flat-file cache (~1.4M files) is approaching filesystem performance limits. A future migration to SQLite-backed cache storage is under consideration.
- GraphQL batch queries are limited to ~50 items per query to stay within GitHub's query complexity limits.
