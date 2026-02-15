# Generic API

In addition to the pipeline commands, the project exposes a generic cached GitHub API client. This is useful for ad-hoc queries and as a library dependency for other projects that need cached GitHub API access without building their own caching infrastructure.

## CLI

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

## Python SDK

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

## GraphQL

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

## `ApiResponse` Fields

| Field    | Type              | Description                                |
|----------|-------------------|--------------------------------------------|
| `status` | `int`             | HTTP status code (200, 201, 204, etc.)     |
| `body`   | `dict` or `list`  | Parsed JSON response body                  |
| `etag`   | `str` or `None`   | ETag header for conditional requests       |
| `link`   | `str` or `None`   | Link header for pagination                 |
