"""
Load raw NBA data from stats.nba.com into DuckDB (data warehouse).
Writes to a local DuckDB file and can export tables to S3 as Parquet.

Raw tables loaded:
- team_game_logs, player_game_logs, team_rosters (core)
- common_all_players, player_info (dimensions)
- schedule, box_summaries (game metadata)

All aggregations (standings, per-game rollups, etc.) are done in dbt.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from load.modules import aws, fetch, utils, warehouse

log = logging.getLogger(__name__)


def main() -> int:
    """Parse CLI, load raw NBA data into DuckDB, optionally export to S3.

    Returns:
        int: Exit code (0 on success).
    """
    parser = argparse.ArgumentParser(description="Load raw NBA data into DuckDB (+ optional S3)")
    parser.add_argument("--season", default="2026", help="Season year (e.g. 2026 for 2025-26)")
    parser.add_argument(
        "--start-season",
        default=None,
        help="Backfill start season year (e.g. 1997). Use with --end-season.",
    )
    parser.add_argument(
        "--end-season",
        default=None,
        help="Backfill end season year (e.g. 2026). Use with --start-season.",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        choices=["Regular Season", "Playoffs", "Pre Season", "All Star"],
        help="NBA API season type",
    )
    parser.add_argument("--db", default="warehouse.duckdb", help="Path to DuckDB file")
    parser.add_argument(
        "--s3-bucket",
        default=os.environ.get("NBA_S3_BUCKET"),
        help="S3 bucket (or NBA_S3_BUCKET)",
    )
    parser.add_argument(
        "--s3-prefix",
        default=os.environ.get("NBA_S3_PREFIX", "nba"),
        help="S3 key prefix",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    seasons = utils.resolve_seasons(args.season, args.start_season, args.end_season)
    log.info(
        "Starting load for %d season(s): %s -> %s (season_type=%s)",
        len(seasons),
        seasons[0],
        seasons[-1],
        args.season_type,
    )

    con = warehouse.init_duckdb(args.db)
    try:
        for i, season in enumerate(seasons, 1):
            log.info("=== Season %s (%d/%d) ===", season, i, len(seasons))
            tables = fetch.load_all_raw(season=season, season_type=args.season_type)
            warehouse.write_duckdb_for_season(
                con, tables, season=season, season_type=args.season_type
            )
    finally:
        con.close()
        log.info("DuckDB connection closed")

    if args.s3_bucket:
        aws.export_to_s3(args.db, args.s3_bucket, args.s3_prefix)
    else:
        log.info("Skipping S3 (set --s3-bucket or NBA_S3_BUCKET to export)")

    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
