"""Simple SQLite database for collection."""

import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).parent.parent / "results" / "files.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    url TEXT PRIMARY KEY,
    sha TEXT,
    size_bytes INTEGER,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS repo_metadata (
    repo_key TEXT PRIMARY KEY,  -- owner/repo
    stars INTEGER,
    forks INTEGER,
    watchers INTEGER,
    language TEXT,
    topics TEXT,  -- JSON array
    created_at TEXT,
    updated_at TEXT,
    pushed_at TEXT,
    default_branch TEXT,
    license TEXT,
    description TEXT,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS file_history (
    url TEXT PRIMARY KEY REFERENCES files(url),
    commits TEXT,  -- JSON array of commit objects
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS search_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    query TEXT NOT NULL,
    size_min INTEGER,
    size_max INTEGER,
    hit_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_search_hits_url ON search_hits(url);

CREATE TABLE IF NOT EXISTS content_status (
    url TEXT PRIMARY KEY REFERENCES files(url),
    status TEXT NOT NULL,  -- 'fetched', 'not_found', 'error'
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scan_progress (
    query TEXT PRIMARY KEY,
    last_lo INTEGER NOT NULL DEFAULT 0,
    max_size INTEGER NOT NULL,
    collected INTEGER NOT NULL DEFAULT 0,
    completed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def init_db(db_path: Path | None = None) -> None:
    """Initialize the database schema."""
    path = db_path or DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()


def get_db(db_path: Path | None = None) -> sqlite3.Connection:
    """Get a database connection."""
    path = db_path or DEFAULT_DB_PATH
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def insert_files(db_path: Path | None, files: list[dict]) -> int:
    """Insert files, ignoring duplicates. Returns count of new files."""
    if not files:
        return 0

    conn = get_db(db_path)
    cursor = conn.cursor()

    new_count = 0
    for f in files:
        try:
            cursor.execute("INSERT INTO files (url, sha) VALUES (?, ?)", (f["html_url"], f["sha"]))
            new_count += 1
        except sqlite3.IntegrityError:
            pass  # Duplicate URL

    conn.commit()
    conn.close()
    return new_count


def get_all_urls(db_path: Path | None = None) -> list[str]:
    """Get all file URLs."""
    conn = get_db(db_path)
    cursor = conn.execute("SELECT url FROM files ORDER BY rowid")
    urls = [row["url"] for row in cursor.fetchall()]
    conn.close()
    return urls


def get_file_count(db_path: Path | None = None) -> int:
    """Get total file count."""
    conn = get_db(db_path)
    count = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    conn.close()
    return count


def get_unique_repos(db_path: Path | None = None) -> list[str]:
    """Get unique repo keys (owner/repo) from file URLs."""
    conn = get_db(db_path)
    # Extract owner/repo from URLs in SQL instead of Python regex over 400K+ rows
    cursor = conn.execute("""
        SELECT DISTINCT
            substr(url, 20, instr(substr(url, 20), '/') - 1)
            || '/' ||
            substr(
                substr(url, 20 + instr(substr(url, 20), '/')),
                1,
                instr(substr(url, 20 + instr(substr(url, 20), '/')), '/') - 1
            ) AS repo_key
        FROM files
        WHERE url LIKE 'https://github.com/%'
    """)
    repos = [row["repo_key"] for row in cursor.fetchall()]
    conn.close()
    return repos


def get_repos_without_metadata(db_path: Path | None = None) -> list[str]:
    """Get repo keys that don't have metadata yet."""
    conn = get_db(db_path)
    cursor = conn.execute("""
        SELECT DISTINCT repo_key FROM (
            SELECT
                substr(url, 20, instr(substr(url, 20), '/') - 1)
                || '/' ||
                substr(
                    substr(url, 20 + instr(substr(url, 20), '/')),
                    1,
                    instr(substr(url, 20 + instr(substr(url, 20), '/')), '/') - 1
                ) AS repo_key
            FROM files
            WHERE url LIKE 'https://github.com/%'
        )
        WHERE repo_key NOT IN (SELECT repo_key FROM repo_metadata)
    """)
    repos = [row["repo_key"] for row in cursor.fetchall()]
    conn.close()
    return repos


def insert_repo_metadata(db_path: Path | None, repo_key: str, metadata: dict) -> None:
    """Insert or update repo metadata."""
    import json

    conn = get_db(db_path)
    conn.execute(
        """
        INSERT OR REPLACE INTO repo_metadata
        (repo_key, stars, forks, watchers, language, topics, created_at, updated_at, pushed_at, default_branch, license, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            repo_key,
            metadata.get("stars"),
            metadata.get("forks"),
            metadata.get("watchers"),
            metadata.get("language"),
            json.dumps(metadata.get("topics", [])),
            metadata.get("created_at"),
            metadata.get("updated_at"),
            metadata.get("pushed_at"),
            metadata.get("default_branch"),
            metadata.get("license"),
            metadata.get("description"),
        ),
    )
    conn.commit()
    conn.close()


def get_files_without_history(db_path: Path | None = None) -> list[str]:
    """Get URLs for files without history."""
    conn = get_db(db_path)
    cursor = conn.execute("""
        SELECT f.url FROM files f
        LEFT JOIN file_history h ON f.url = h.url
        WHERE h.url IS NULL
    """)
    results = [row["url"] for row in cursor.fetchall()]
    conn.close()
    return results


def insert_repo_metadata_batch(db_path: Path | None, items: list[tuple[str, dict]]) -> None:
    """Insert or update repo metadata in a single transaction.

    Each item is (repo_key, metadata_dict).
    """
    if not items:
        return
    import json

    conn = get_db(db_path)
    conn.executemany(
        """
        INSERT OR REPLACE INTO repo_metadata
        (repo_key, stars, forks, watchers, language, topics, created_at, updated_at, pushed_at, default_branch, license, description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        [
            (
                repo_key,
                m.get("stars"),
                m.get("forks"),
                m.get("watchers"),
                m.get("language"),
                json.dumps(m.get("topics", [])),
                m.get("created_at"),
                m.get("updated_at"),
                m.get("pushed_at"),
                m.get("default_branch"),
                m.get("license"),
                m.get("description"),
            )
            for repo_key, m in items
        ],
    )
    conn.commit()
    conn.close()


def insert_file_history(db_path: Path | None, url: str, commits: list[dict]) -> None:
    """Insert file history."""
    import json

    conn = get_db(db_path)
    conn.execute(
        "INSERT OR REPLACE INTO file_history (url, commits) VALUES (?, ?)",
        (url, json.dumps(commits)),
    )
    conn.commit()
    conn.close()


def insert_file_history_batch(db_path: Path | None, items: list[tuple[str, list[dict]]]) -> None:
    """Insert file history in a single transaction.

    Each item is (url, commits_list).
    """
    if not items:
        return
    import json

    conn = get_db(db_path)
    conn.executemany(
        "INSERT OR REPLACE INTO file_history (url, commits) VALUES (?, ?)",
        [(url, json.dumps(commits)) for url, commits in items],
    )
    conn.commit()
    conn.close()


def insert_search_hits(db_path: Path | None, hits: list[dict]) -> None:
    """Insert search hits for tracking. Each hit is {url, query, size_min, size_max}."""
    if not hits:
        return
    conn = get_db(db_path)
    conn.executemany(
        "INSERT INTO search_hits (url, query, size_min, size_max) VALUES (?, ?, ?, ?)",
        [(h["url"], h["query"], h.get("size_min"), h.get("size_max")) for h in hits],
    )
    conn.commit()
    conn.close()


def get_scan_progress(db_path: Path | None, query: str) -> dict | None:
    """Get scan progress for a query. Returns dict with last_lo, max_size, collected, completed_at."""
    conn = get_db(db_path)
    row = conn.execute(
        "SELECT last_lo, max_size, collected, completed_at FROM scan_progress WHERE query = ?",
        (query,),
    ).fetchone()
    conn.close()
    if row:
        return {
            "last_lo": row["last_lo"],
            "max_size": row["max_size"],
            "collected": row["collected"],
            "completed_at": row["completed_at"],
        }
    return None


def update_scan_progress(
    db_path: Path | None, query: str, last_lo: int, max_size: int, collected: int, completed: bool = False
) -> None:
    """Update scan progress for a query."""
    conn = get_db(db_path)
    conn.execute(
        """INSERT OR REPLACE INTO scan_progress (query, last_lo, max_size, collected, completed_at, updated_at)
        VALUES (?, ?, ?, ?, CASE WHEN ? THEN CURRENT_TIMESTAMP ELSE NULL END, CURRENT_TIMESTAMP)""",
        (query, last_lo, max_size, collected, completed),
    )
    conn.commit()
    conn.close()


def get_files_without_content(db_path: Path | None = None) -> list[str]:
    """Get URLs for files without content status."""
    conn = get_db(db_path)
    cursor = conn.execute("""
        SELECT f.url FROM files f
        LEFT JOIN content_status c ON f.url = c.url
        WHERE c.url IS NULL
    """)
    results = [row["url"] for row in cursor.fetchall()]
    conn.close()
    return results


def insert_content_status_batch(db_path: Path | None, items: list[tuple[str, str]]) -> None:
    """Insert content status in a single transaction.

    Each item is (url, status) where status is 'fetched', 'not_found', or 'error'.
    """
    if not items:
        return
    conn = get_db(db_path)
    conn.executemany(
        "INSERT OR REPLACE INTO content_status (url, status) VALUES (?, ?)",
        items,
    )
    conn.commit()
    conn.close()


def get_multi_range_hits(db_path: Path | None = None) -> list[dict]:
    """Find URLs that were returned by multiple different size ranges."""
    conn = get_db(db_path)
    cursor = conn.execute("""
        SELECT url, COUNT(DISTINCT size_min || '-' || size_max) as range_count,
               GROUP_CONCAT(DISTINCT size_min || '..' || size_max) as ranges
        FROM search_hits
        GROUP BY url
        HAVING range_count > 1
        ORDER BY range_count DESC
    """)
    results = [
        {"url": row["url"], "range_count": row["range_count"], "ranges": row["ranges"]}
        for row in cursor.fetchall()
    ]
    conn.close()
    return results
