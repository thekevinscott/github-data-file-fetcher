# Pipeline Commands

The pipeline has four purpose-built commands that work together. Run them in order: collect file paths, then fetch content/metadata/history.

## `fetch-file-paths`

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

## `fetch-file-content`

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

## `fetch-repo-metadata`

Fetches repository metadata (stars, forks, description, topics, license, language) for all repos in the database.

```bash
uv run github-fetch fetch-repo-metadata

# GraphQL batch mode
uv run github-fetch fetch-repo-metadata --graphql --batch-size 50

# Custom output
uv run github-fetch fetch-repo-metadata --db /path/to/db.db -o metadata.json
```

**Output**: `results/repo_metadata.json`

## `fetch-file-history`

Fetches commit history for each file (first/last commit dates, authors, total commits).

```bash
uv run github-fetch fetch-file-history

# GraphQL batch mode
uv run github-fetch fetch-file-history --graphql --batch-size 20

# Custom output
uv run github-fetch fetch-file-history --db /path/to/db.db -o history.json
```

**Output**: `results/file_history.json`
