# GitHub Data File Fetcher

Collect files from GitHub at scale, with aggressive caching and rate-limit handling. Also provides a generic cached GitHub API client usable as a library by other projects.

## Why not just use the GitHub API directly?

GitHub limits code search results to 1,000 files per query. This tool shards queries by file size using a linear scan with adaptive chunk sizing to retrieve dramatically more results. All API responses are cached locally, so re-runs complete in seconds.

## Setup

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

Set your GitHub token (required for all commands):

```bash
export GITHUB_TOKEN=ghp_...
```

Or create a `.env` file:

```
GITHUB_TOKEN=ghp_...
```

## Pipeline Commands

The pipeline has four purpose-built commands that work together. Run them in order: collect file paths, then fetch content/metadata/history.

### `fetch-file-paths`

Collects file paths matching a search query using linear scanning with adaptive chunks. Stores results in SQLite.

```bash
uv run github-fetch fetch-file-paths "filename:SKILL.md"
uv run github-fetch fetch-file-paths "filename:auth.jsx"

# Custom database location
uv run github-fetch fetch-file-paths --db /tmp/test.db "filename:config.ts"

# Skip cache for fresh collection
uv run github-fetch fetch-file-paths "filename:SKILL.md" --skip-cache
```

**Output**: `results/files.db` -- SQLite database with URLs, SHAs, and repo info.

### `fetch-file-content`

Downloads file content for all URLs in the database. Supports both REST (one file at a time) and GraphQL batch mode (~50x faster).

```bash
# REST mode (default)
uv run github-fetch fetch-file-content

# GraphQL batch mode (recommended for large datasets)
uv run github-fetch fetch-file-content --graphql --batch-size 50

# Custom paths
uv run github-fetch fetch-file-content --db /path/to/db.db --content-dir /path/to/content
```

**Output**: `results/content/` -- Files organized by `owner/repo/blob/ref/path`.

### `fetch-repo-metadata`

Fetches repository metadata (stars, forks, description, topics, license, language) for all repos in the database.

```bash
uv run github-fetch fetch-repo-metadata

# GraphQL batch mode
uv run github-fetch fetch-repo-metadata --graphql --batch-size 50

# Custom output
uv run github-fetch fetch-repo-metadata --db /path/to/db.db -o metadata.json
```

**Output**: `results/repo_metadata.json`

### `fetch-file-history`

Fetches commit history for each file (first/last commit dates, authors, total commits).

```bash
uv run github-fetch fetch-file-history

# GraphQL batch mode
uv run github-fetch fetch-file-history --graphql --batch-size 20

# Custom output
uv run github-fetch fetch-file-history --db /path/to/db.db -o history.json
```

**Output**: `results/file_history.json`

## Generic API

In addition to the pipeline commands, the project exposes a generic cached GitHub API client. This is useful for ad-hoc queries and as a library dependency for other projects that need cached GitHub API access without building their own caching infrastructure.

### CLI

```bash
# Fetch a repo's directory listing
uv run github-fetch api repos/anthropics/claude-code/contents/.claude

# With query parameters
uv run github-fetch api search/code --param "q=filename:SKILL.md" --param per_page=10

# Pipe to jq for filtering
uv run github-fetch api repos/anthropics/claude-code | jq '.stargazers_count'

# Skip cache (force fresh fetch)
uv run github-fetch api repos/anthropics/claude-code --skip-cache

# GraphQL queries
uv run github-fetch api graphql --graphql --query '{ viewer { login } }'
```

Output is raw JSON to stdout. Second call to the same endpoint returns from cache in milliseconds.

### Python SDK

Install as a dependency in another project:

```bash
uv add github-data-file-fetcher --editable /path/to/github-data-file-fetcher
```

Then use:

```python
from github_data_file_fetcher import get_generic_client, ApiResponse

client = get_generic_client()

# REST -- cached, throttled, rate-limited automatically
resp = client.api("repos/owner/repo/contents/path")
# Returns: ApiResponse(status=200, body={...}, etag="...", link="...")

# Access response fields
print(resp.body)    # parsed JSON (dict or list)
print(resp.status)  # HTTP status code
print(resp.etag)    # ETag header (for conditional requests)
print(resp.link)    # Link header (for pagination)

# With query params
resp = client.api("search/code", params={"q": "filename:SKILL.md", "per_page": 100})

# Skip cache for this call (still writes to cache)
resp = client.api("repos/owner/repo", skip_cache=True)

# Non-GET requests (never cached)
resp = client.api("repos/owner/repo/forks", method="POST")
```

GraphQL:

```python
from github_data_file_fetcher.graphql import GraphQLClient

gql = GraphQLClient()
result = gql.graphql("""
    query {
        repository(owner: "anthropics", name: "claude-code") {
            stargazerCount
            defaultBranchRef { name }
        }
    }
""")
print(result["data"]["repository"]["stargazerCount"])
```

### `ApiResponse` fields

| Field    | Type              | Description                                |
|----------|-------------------|--------------------------------------------|
| `status` | `int`             | HTTP status code (200, 201, 204, etc.)     |
| `body`   | `dict` or `list`  | Parsed JSON response body                  |
| `etag`   | `str` or `None`   | ETag header for conditional requests       |
| `link`   | `str` or `None`   | Link header for pagination                 |

## Caching

All API responses are cached to `~/.cache/github-data-file-fetcher/` as flat JSON files. The cache currently holds ~1.4M entries (~11GB).

**Key behaviors:**

- **Cache keys**: `sha256(endpoint|params)[:16].json` -- stable across restarts.
- **Generic API / GraphQL**: Uses [Cachetta](https://pypi.org/project/cachetta/) `@cache` decorator. 30-day TTL. Only 2xx responses cached. Errors and non-GET requests are never cached.
- **Pipeline commands**: Use a legacy `Cache` class. No TTL (data treated as immutable). Some error states cached (e.g., 404s for files that don't exist).
- **Re-runs**: Cached items are skipped in milliseconds. A killed-and-restarted pipeline resumes from where it left off.
- **`skip_cache`**: Bypasses cache reads but still writes, so the fresh result is available on the next run.

## Rate Limiting

Both REST and GraphQL clients handle GitHub rate limits automatically:

- **REST** (PyGithub + generic client): Steady-state throttle at 1.3 req/sec (~4,680/hour, under the 5,000/hour limit). Sleeps on 429/403 rate-limit responses. Exponential backoff on 5xx errors.
- **GraphQL**: 30 queries/sec (~1,800/min, under the 2,000/min secondary limit). Respects `Retry-After` headers.

No manual intervention needed. Long-running pipelines pause when rate-limited and resume automatically.

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
