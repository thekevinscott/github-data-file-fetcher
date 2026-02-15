# GitHub Data File Fetcher

## Project Structure

```
github_data_file_fetcher/
  __init__.py              # Entry point
  cli.py                   # CLI commands
  db.py                    # SQLite schema and connection helpers

  # Data fetching modules
  fetch_file_paths/        # Linear scan collection
  fetch_file_content/      # Content fetching
  fetch_repo_metadata/     # Repository metadata
  fetch_file_history/      # Commit history

  # Shared
  models.py                # Constants and ApiResponse dataclass
  github.py                # PyGithub-based client with caching and rate limiting
  generic_client.py        # Generic httpx-based client for arbitrary API calls
  graphql.py               # GraphQL batch client + generic graphql() method
  utils.py                 # Shared utilities (status output, URL parsing)
  settings.py              # Settings/config loading
```

## Pipeline

Uses linear scanning with adaptive chunk sizes for collection. Stores results in SQLite for efficient querying.

**Commands:**
```bash
uv run github-fetch fetch-file-paths      # Collect file paths
uv run github-fetch fetch-file-content    # Download file content
uv run github-fetch fetch-repo-metadata   # Get repo metadata
uv run github-fetch fetch-file-history    # Get commit history
uv run github-fetch api <endpoint>       # Generic cached API call
```

## Generic API (Library + CLI)

For arbitrary cached GitHub API calls. Used by this project and downstream consumers (e.g., skillet).

### Python SDK

```python
from github_data_file_fetcher import get_generic_client, ApiResponse

client = get_generic_client()

# REST -- cached, throttled, rate-limited
resp = client.api("repos/owner/repo/contents/path")
# Returns: ApiResponse(status=200, body={...}, etag="...", link="...")

# With query params
resp = client.api("search/code", params={"q": "filename:SKILL.md", "per_page": 100})

# Skip cache (force fresh)
resp = client.api("repos/owner/repo", skip_cache=True)

# Non-GET (never cached)
resp = client.api("repos/owner/repo/forks", method="POST")
```

`ApiResponse` fields: `status` (int), `body` (dict|list), `etag` (str|None), `link` (str|None).

### GraphQL

```python
from github_data_file_fetcher.graphql import GraphQLClient

gql = GraphQLClient()
result = gql.graphql("{ viewer { login } }")
# Returns: {"data": {"viewer": {"login": "..."}}}
```

### CLI

```bash
uv run github-fetch api repos/owner/repo/contents/.claude
uv run github-fetch api search/code --param "q=filename:SKILL.md" --param per_page=100
uv run github-fetch api repos/owner/repo --skip-cache
uv run github-fetch api graphql --graphql --query '{ viewer { login } }'
```

Output: JSON to stdout. Pipe to `jq`, redirect to file, etc.

### Caching behavior

- Only 2xx GET responses are cached. Non-GET and errors are never cached.
- Cache format: `{"status", "body", "etag", "link"}` wrapper (distinct from purpose-built methods which store bare JSON bodies).
- Cache keys: `sha256(endpoint|params)[:16]`, same flat-file directory as other methods (`~/.cache/github-data-file-fetcher/`).
- TTL: 30 days (via Cachetta). Use `skip_cache=True` to force refresh.
- Caching backed by [Cachetta](https://pypi.org/project/cachetta/) `@cache` decorator on inner fetch functions; purpose-built methods still use the legacy `Cache` class.

## Running Tests

```bash
uv run pytest
```


## Marimo Notebooks

Analysis notebooks are in `notebooks/`.

### Running the Server

For remote access (e.g., via Tailscale), bind to all interfaces:

```bash
uv run marimo edit notebooks/ --port 2718 --watch --host 0.0.0.0
```

- `--host 0.0.0.0`: Required for remote access (default is localhost only)
- `--watch`: Auto-reload when notebook files change on disk
- `--port 2718`: Custom port to avoid conflicts

### Capturing Notebook Output for Claude

Use the `/marimo-screenshot` skill to capture rendered notebook output (charts, tables) as a PNG that Claude can analyze.

See: `.claude/skills/marimo-screenshot/SKILL.md`

**Quick version:**
```bash
uv run marimo run notebooks/<notebook>.py --port 2719 --headless &
sleep 10
npx playwright screenshot --wait-for-timeout 15000 --full-page \
  "http://localhost:2719" /tmp/marimo_output.png
# Then read /tmp/marimo_output.png
pkill -f "marimo run.*2719"
```

Key insight: `marimo run` (not `edit`) executes cells and serves rendered output.

### Verification Rule

**After every change to a marimo notebook:**

1. **Verify syntax:**
   ```bash
   python -m py_compile notebooks/<notebook>.py
   ```

2. **Verify rendered output** (check for runtime errors like output size limits):
   ```bash
   pkill -f "marimo run.*2719" 2>/dev/null; true
   uv run marimo run notebooks/<notebook>.py --port 2719 --headless &
   sleep 15
   # Use tall viewport to capture full notebook (--full-page doesn't work with marimo)
   npx playwright screenshot --viewport-size="1280,3000" --wait-for-timeout 15000 \
     "http://localhost:2719" /tmp/notebook_check.png
   # Read /tmp/notebook_check.png to verify no errors
   pkill -f "marimo run.*2719"
   ```

Common runtime errors to watch for:
- **"Your output is too large"** - Sample data for charts (max ~5000 rows)
- **MaxRowsError** - Add `alt.data_transformers.disable_max_rows()`
- **Blank sections** - Check cell return values

### Variable Naming Convention

Marimo requires unique variable names across all cells. Use underscore-prefixed names (`_fig`, `_ax`, `_colors`) for local variables that don't need to be exported to other cells.

## Shared Resources - DO NOT MODIFY

- `.cache/` is shared across processes. NEVER delete, clear, or modify without explicit user permission.
- When testing, use isolated directories (e.g., `tmp_path` fixtures) instead of touching production caches.

## Library Migrations & New Dependencies

- Before using a library's API, read its documentation for pagination, defaults, and configuration.
- Do not assume library behavior - verify with targeted tests or source code inspection.
- When migrating from one tool to another (e.g., CLI to SDK), write comparison tests proving identical behavior.

## API Limits and Boundaries

- When code handles limits (e.g., "max 1000 results"), always use `>=` not `>` for the boundary check.
- Write explicit tests for: limit-1, exactly limit, limit+1.
- Add comments explaining why boundary checks use `>=` when truncation is possible.

## Destructive Actions

- Never delete files outside of `tmp/` or test directories without explicit user permission.
- "Clear and retry" is not a safe debugging strategy for shared resources.
- Ask before deleting: "Can I clear X?" - even if it seems like the obvious fix.
