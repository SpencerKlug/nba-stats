"""
Load raw NCAA men's basketball data from stats.ncaa.org into DuckDB.

Usage:
  python -m load.ncaa --season 2026 --db warehouse.duckdb
  python -m load.ncaa --start-season 2022 --end-season 2026
  python -m load.ncaa --limit 10
"""

from __future__ import annotations

import argparse
import logging
import sys

from load.modules import utils, warehouse
from load.ncaa import load_ncaa_mbb_season

log = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load NCAA men's basketball data from stats.ncaa.org into DuckDB"
    )
    parser.add_argument("--season", default="2026", help="Season end year (e.g. 2026 for 2025-26)")
    parser.add_argument("--start-season", default=None, help="Backfill start. Use with --end-season.")
    parser.add_argument("--end-season", default=None, help="Backfill end. Use with --start-season.")
    parser.add_argument("--db", default="warehouse.duckdb", help="Path to DuckDB file")
    parser.add_argument("--skip-team-list", action="store_true", help="Do not fetch team list")
    parser.add_argument("--skip-games", action="store_true", help="Do not fetch game schedule")
    parser.add_argument("--skip-box-scores", action="store_true", help="Do not fetch player box scores")
    parser.add_argument("--use-team-schedules", action="store_true", help="Use team schedules (fallback)")
    parser.add_argument("--limit", type=int, default=None, metavar="N", help="Cap games for testing")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    seasons = utils.resolve_seasons(args.season, args.start_season, args.end_season)
    log.info("NCAA MBB load: %d season(s) %s -> %s", len(seasons), seasons[0], seasons[-1])

    con = warehouse.init_duckdb(args.db)
    try:
        for i, season in enumerate(seasons, 1):
            log.info("=== NCAA season %s (%d/%d) ===", season, i, len(seasons))
            tables = load_ncaa_mbb_season(
                season=season,
                include_team_list=not args.skip_team_list,
                include_games=not args.skip_games,
                include_box_scores=not args.skip_box_scores,
                use_team_schedules=args.use_team_schedules,
                limit=args.limit,
            )
            warehouse.write_duckdb_for_season(con, tables, season=season, season_type=None)
    finally:
        con.close()
        log.info("DuckDB connection closed")

    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
