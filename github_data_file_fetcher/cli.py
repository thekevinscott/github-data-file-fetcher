"""CLI commands for skill collection."""

import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Collect files from GitHub",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent.parent / "results",
        help="Output directory for results",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # fetch-file-paths subcommand
    fetch_parser = subparsers.add_parser(
        "fetch-file-paths",
        help="Fetch file paths matching query",
    )
    fetch_parser.add_argument(
        "query",
        help="Search query (e.g., filename:SKILL.md)",
    )
    fetch_parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Database path (default: results/files.db)",
    )
    fetch_parser.add_argument(
        "--skip-cache",
        action="store_true",
        help="Skip reading from cache (still writes to cache)",
    )

    # fetch-file-content subcommand
    content_parser = subparsers.add_parser(
        "fetch-file-content",
        help="Fetch content for collected file paths",
    )
    content_parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Database path (default: results/files.db)",
    )
    content_parser.add_argument(
        "--content-dir",
        type=Path,
        default=None,
        help="Directory to store content (default: results/content)",
    )
    content_parser.add_argument(
        "--graphql",
        action="store_true",
        help="Use GraphQL batch API (separate rate limit pool, ~50x faster)",
    )
    content_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Files per GraphQL query (default: 50, requires --graphql)",
    )

    # fetch-repo-metadata subcommand
    meta_parser = subparsers.add_parser(
        "fetch-repo-metadata",
        help="Fetch repository metadata (stars, forks, etc.)",
    )
    meta_parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Database path (default: results/files.db)",
    )
    meta_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output JSON file (default: results/repo_metadata.json)",
    )
    meta_parser.add_argument(
        "--graphql",
        action="store_true",
        help="Use GraphQL batch API (separate rate limit pool, ~50x faster)",
    )
    meta_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Repos per GraphQL query (default: 50, requires --graphql)",
    )

    # fetch-file-history subcommand
    history_parser = subparsers.add_parser(
        "fetch-file-history",
        help="Fetch commit history for skill files",
    )
    history_parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Database path (default: results/files.db)",
    )
    history_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output JSON file (default: results/file_history.json)",
    )
    history_parser.add_argument(
        "--graphql",
        action="store_true",
        help="Use GraphQL batch API (separate rate limit pool, ~50x faster)",
    )
    history_parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Files per GraphQL query (default: 20, requires --graphql)",
    )

    # api subcommand
    api_parser = subparsers.add_parser(
        "api",
        help="Make a generic cached GitHub API call",
    )
    api_parser.add_argument(
        "endpoint",
        help="API endpoint path (e.g., repos/owner/repo/contents/path)",
    )
    api_parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Query parameter (repeatable, e.g., --param per_page=100)",
    )
    api_parser.add_argument(
        "--method",
        default="GET",
        help="HTTP method (default: GET)",
    )
    api_parser.add_argument(
        "--skip-cache",
        action="store_true",
        help="Skip cache for this call",
    )
    api_parser.add_argument(
        "--graphql",
        action="store_true",
        help="Treat endpoint as GraphQL (use --query instead of positional endpoint)",
    )
    api_parser.add_argument(
        "--query",
        default=None,
        help="GraphQL query string (requires --graphql)",
    )

    args = parser.parse_args()

    if args.command == "fetch-file-paths":
        from .fetch_file_paths import fetch_file_paths

        db_path = args.db or (args.output_dir / "files.db")
        fetch_file_paths(args.query, db_path=db_path, skip_cache=args.skip_cache)
    elif args.command == "fetch-file-content":
        from .db import get_file_count, get_files_without_content, init_db

        db_path = args.db or (args.output_dir / "files.db")
        init_db(db_path)
        urls = get_files_without_content(db_path)
        total = get_file_count(db_path)
        content_dir = args.content_dir or (args.output_dir / "content")

        if not urls:
            print(f"All {total:,} files already have content status. Nothing to fetch.")
        else:
            print(f"Fetching content for {len(urls):,} URLs ({total - len(urls):,} already done) to {content_dir}")

            if args.graphql:
                from .fetch_file_content import fetch_file_content_graphql

                stats = fetch_file_content_graphql(urls, content_dir, db_path=db_path, batch_size=args.batch_size)
                print(
                    f"\nDone: {stats['fetched']} fetched, {stats['errors']} errors, "
                    f"{stats['truncated_rest']} REST fallback, {stats['queries']} queries"
                )
            else:
                from .fetch_file_content import fetch_file_content

                stats = fetch_file_content(urls, content_dir, db_path=db_path)
                print(f"\nDone: {stats['fetched']} fetched, {stats['errors']} errors")
    elif args.command == "fetch-repo-metadata":
        db_path = args.db or (args.output_dir / "files.db")

        if args.graphql:
            from .fetch_repo_metadata import fetch_repo_metadata_graphql

            stats = fetch_repo_metadata_graphql(db_path=db_path, batch_size=args.batch_size)
            print(
                f"\nDone: {stats['fetched']} fetched, {stats['errors']} errors, "
                f"{stats['queries']} queries"
            )
        else:
            from .fetch_repo_metadata import fetch_repo_metadata

            stats = fetch_repo_metadata(db_path=db_path)
            print(f"\nDone: {stats['fetched']} fetched, {stats['errors']} errors")
    elif args.command == "fetch-file-history":
        db_path = args.db or (args.output_dir / "files.db")

        if args.graphql:
            from .fetch_file_history import fetch_file_history_graphql

            stats = fetch_file_history_graphql(db_path=db_path, batch_size=args.batch_size)
            print(
                f"\nDone: {stats['fetched']} fetched, {stats['errors']} errors, "
                f"{stats['queries']} queries"
            )
        else:
            from .fetch_file_history import fetch_file_history

            stats = fetch_file_history(db_path=db_path)
            print(f"\nDone: {stats['fetched']} fetched, {stats['errors']} errors")
    elif args.command == "api":
        import json
        import sys

        if args.graphql:
            from . import graphql as graphql_mod

            query_str = args.query or args.endpoint
            gql = graphql_mod.GraphQLClient()
            try:
                result = gql.graphql(query_str)
                json.dump(result, sys.stdout, indent=2)
                sys.stdout.write("\n")
            finally:
                gql.close()
        else:
            from . import generic_client

            params = {}
            for p in args.param:
                k, _, v = p.partition("=")
                params[k] = v

            client = generic_client.get_generic_client(skip_cache=args.skip_cache)
            resp = client.api(args.endpoint, params=params or None, method=args.method)
            json.dump(resp.body, sys.stdout, indent=2)
            sys.stdout.write("\n")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
