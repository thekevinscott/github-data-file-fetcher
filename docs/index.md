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

## Quick Start

```bash
# Collect file paths matching a query
uv run github-fetch fetch-file-paths "filename:SKILL.md"

# Download file content
uv run github-fetch fetch-file-content --graphql --batch-size 50

# Get repo metadata
uv run github-fetch fetch-repo-metadata --graphql

# Ad-hoc API call (cached)
uv run github-fetch api repos/anthropics/claude-code | jq '.stargazers_count'
```

See [Pipeline Commands](pipeline.md) for the full pipeline, or [Generic API](generic-api.md) for the cached API client.
