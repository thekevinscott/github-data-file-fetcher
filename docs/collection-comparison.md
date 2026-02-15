# GitHub File Collection: v1 vs v3 Comparison

## Background

GitHub's Code Search API enforces a hard limit of 1,000 results per query. To collect all files matching a query like `filename:tsup.config.ts`, the search space must be partitioned into chunks that each return fewer than 1,000 results. The standard technique is **size sharding**: appending `size:X..Y` to queries to restrict results by file size in bytes.

This document compares two implementations of size-sharded collection and their results against GitHub's reported `total_count`.

## Collection Approaches

### v1: Predefined Size Ranges with Binary Split

v1 uses a set of predefined `SIZE_RANGES` that cover 0 to 1MB. When a query against a range returns >= 1,000 results (the pagination limit), v1 splits that range in half and retries each half recursively. Results are written to a flat text file (`skill_urls.txt`), one URL per line.

**Characteristics:**
- Starts with wide ranges, narrows only when forced by overflow
- Uses `skill_files.json` as a checkpoint for resumability
- Stores results as plain text URLs

### v3: Linear Scan with Adaptive Chunking

v3 scans linearly from size 0 upward through the full file size space. It starts with a chunk size (e.g., 100 bytes) and adapts: when a chunk returns >= 1,000 results, it narrows the range; when results are sparse, it widens. Results are stored in SQLite with deduplication at insert time.

**Characteristics:**
- Systematically covers every byte of the size space
- Adaptive chunk sizing responds to local density
- SQLite storage enables efficient querying and deduplication
- Narrower ranges by default than v1, even in sparse regions

## Methodology

Nine filename queries were selected spanning a range of GitHub API `total_count` values from 3 to 6,776. Both v1 and v3 were run with `--skip-cache` to ensure fresh API responses and a fair comparison. All runs used the same GitHub token and ran concurrently from the same machine.

### Metrics

- **API**: GitHub's reported `total_count` for the unsharded query
- **v1**: Unique files collected by v1
- **v3**: Unique files collected by v3
- **v1 in v3**: Percentage of v1's files that also appear in v3's results
- **v3/v1**: How many more files v3 found relative to v1
- **v3/API**: How many more files v3 found relative to GitHub's reported count

## Results

All nine tests ran to completion for both v1 and v3.

| Test | Filename | API | v1 | v3 | v1 in v3 | v3/v1 | v3/API |
|------|----------|----:|---:|---:|----------|------:|-------:|
| zustand | zustand.config.js | 3 | 2 | 3 | 100.00% | +50% | +0% |
| oxlint | oxlint.config.json | 29 | 22 | 29 | 100.00% | +32% | +0% |
| mise | mise.local.toml | 101 | 94 | 101 | 100.00% | +7% | +0% |
| knip | knip.jsonc | 292 | 318 | 424 | 99.06% | +33% | +45% |
| rsbuild | rsbuild.config.ts | 976 | 3,055 | 3,533 | 99.71% | +16% | +262% |
| wrangler | wrangler.toml | 2,336 | 13,909 | 42,965 | 99.95% | +209% | +1739% |
| vite | vite.config.mts | 2,376 | 9,029 | 13,279 | 99.45% | +47% | +459% |
| auth | auth.jsx | 4,044 | 20,445 | 64,659 | 87.67% | +216% | +1499% |
| tsup | tsup.config.ts | 6,776 | 9,244 | 41,893 | 99.75% | +353% | +518% |

## Findings

### 1. GitHub's `total_count` is unreliable and undercounts

The API's reported `total_count` consistently understates the actual number of matching files. For small datasets (< 100 files), the count is roughly accurate. As the true file count increases, the undercount becomes severe:

| API Range | Observed v3/API |
|-----------|-----------------|
| < 100 | +0% to +7% |
| 100-1,000 | +45% to +262% |
| 1,000-7,000 | +459% to +1,739% |

At the high end, v3 found **18x more files** than the API reported for `wrangler.toml` (42,965 vs 2,336).

### 2. Narrow size sharding recovers files invisible to wider queries

GitHub's search index drops files from results as query ranges widen, even when the result count is well below the 1,000-result limit. This is not a pagination issue -- it appears to be index-level pruning.

**Example**: `supabase/supabase/knip.jsonc` (551 bytes)
- Found in `size:400..700` (90 results)
- NOT found in `size:0..10000` (421 results)

This means files can be invisible to a query that should contain them, simply because the size range is too broad. v3's narrow scanning recovers these files; v1's wider initial ranges miss them.

### 3. v3 is a near-superset of v1

For 8 of 9 completed tests, v3 captured 99.06% to 100% of the files found by v1. v3 finds everything v1 finds, plus substantially more.

The one exception is `auth.jsx` at 87.67% overlap. This is likely due to suffix matching (`filename:auth.jsx` matches `RequireAuth.jsx`, `useAuth.jsx`, etc.), which creates a much more diverse result set. GitHub's non-deterministic pruning affects diverse result sets more than uniform ones.

### 4. v1 also exceeds API counts, but by less

v1's binary-split approach also finds more files than the API reports, just not as many as v3. This confirms the issue is with GitHub's search index, not with the collection tool. v3 simply exploits narrower queries more aggressively.

| Test | API | v1/API | v3/API |
|------|----:|-------:|-------:|
| knip | 292 | +9% | +45% |
| rsbuild | 976 | +213% | +262% |
| wrangler | 2,336 | +496% | +1,739% |
| vite | 2,376 | +280% | +459% |
| auth | 4,044 | +406% | +1,499% |
| tsup | 6,776 | +36% | +518% |

### 5. The v3/API ratio increases with dataset size

Larger datasets benefit more from narrow sharding. The relationship is noisy (wrangler at +1,739% vs vite at +459% despite similar API counts), but the trend is clear: as the true number of files grows, the gap between what GitHub reports and what v3 recovers widens.

## Implications

1. **Do not trust `total_count`** for planning or progress estimation. It can undercount by an order of magnitude.

2. **Narrow size ranges are essential** for comprehensive collection. Even if a query returns < 1,000 results, widening the size range can cause files to disappear from results.

3. **Linear scanning outperforms binary splitting** because it systematically covers the entire size space with narrow ranges, rather than starting wide and only narrowing when forced.

4. **Result sets are non-deterministic.** The same query at different times may surface different files. Running both v1 and v3 concurrently on `auth.jsx` produced an 87.67% overlap, meaning 12.33% of v1's files were never seen by v3 despite v3 using narrower ranges. Multiple passes or complementary strategies may be needed for exhaustive collection.

## Reproducing

```bash
# v3 collection (current approach)
cd ~/work/github-data-file-fetcher
uv run github-fetch fetch-file-paths --db /tmp/output.db "filename:QUERY"

# With --skip-cache for fresh API data
uv run github-fetch fetch-file-paths --skip-cache --db /tmp/output.db "filename:QUERY"
```

Results are stored in SQLite. Query with:
```sql
SELECT COUNT(*) FROM files;
SELECT url FROM files LIMIT 10;
```
