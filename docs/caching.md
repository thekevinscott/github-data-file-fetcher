# Caching & Rate Limiting

## Caching

All API responses are cached to `~/.cache/github-data-file-fetcher/` as flat JSON files.

### Key behaviors

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
