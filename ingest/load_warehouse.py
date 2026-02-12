"""
Load raw NBA data from Basketball-Reference into DuckDB (data warehouse).
Writes to a local DuckDB file and can export tables to S3 as Parquet.

Raw only: games, player_season_totals, roster. All aggregations (standings,
per-game stats, etc.) are done in dbt.

Usage:
  python -m ingest.load_warehouse --season 2026
  python -m ingest.load_warehouse --season 2026 --s3-bucket my-bucket --s3-prefix nba/raw
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd
import requests

# Add project root for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from basketball_reference import (  # noqa: E402
    NBA_TEAM_ABBREVS,
    player_season_totals,
    schedule_results,
    team_roster,
)

log = logging.getLogger(__name__)


def to_snake_case(s: str) -> str:
    """Convert column name to snake_case (e.g. 'W/L%' -> 'w_l_pct')."""
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", "_", s.strip()).lower()
    return s or "unknown"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame columns to snake_case; deduplicate with _1, _2 suffix."""
    df = df.copy()
    base = [to_snake_case(str(c)) for c in df.columns]
    seen: dict[str, int] = {}
    out = []
    for name in base:
        if name in seen:
            seen[name] += 1
            out.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            out.append(name)
    df.columns = out
    return df


def ensure_season_column(df: pd.DataFrame, season: str) -> pd.DataFrame:
    """Add season column if missing (e.g. 2026 for 2025-26)."""
    if "season" not in df.columns:
        df = df.assign(season=season)
    return df


# Month suffixes used by Basketball-Reference for schedule pages
SCHEDULE_MONTHS = [
    "october", "november", "december", "january", "february",
    "march", "april", "may", "june",
]


def load_games(season: str) -> pd.DataFrame:
    """Load all games for a season (raw schedule/results). Fetches each month and concatenates."""
    log.info("Loading games for season %s...", season)
    frames = []
    for month in SCHEDULE_MONTHS:
        try:
            df = schedule_results(season=season, month=month)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log.debug("Schedule not found for month=%s (404), skipping", month)
                continue  # Month page doesn't exist (e.g. May/June for in-progress season)
            raise
        if df.empty:
            log.debug("No games for month=%s, skipping", month)
            continue
        log.info("  %s: %d games", month, len(df))
        df = normalize_columns(df)
        df = ensure_season_column(df, season)
        frames.append(df)
    if not frames:
        log.info("No monthly schedule pages found, trying main schedule page...")
        try:
            df = schedule_results(season=season)
        except requests.exceptions.HTTPError:
            log.warning("Main schedule page also failed")
            return pd.DataFrame()
        if not df.empty:
            df = normalize_columns(df)
            df = ensure_season_column(df, season)
            log.info("  main: %d games", len(df))
            return df
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    # Dedupe by date + visitor + home in case of overlap
    key = ["date", "visitor_neutral", "home_neutral"] if "visitor_neutral" in out.columns else ["date"]
    if all(c in out.columns for c in key):
        before = len(out)
        out = out.drop_duplicates(subset=key, keep="first")
        if len(out) < before:
            log.debug("Dropped %d duplicate game rows", before - len(out))
    log.info("Games total: %d rows", len(out))
    return out


def load_player_totals(season: str) -> pd.DataFrame:
    """Load raw player season totals (no per-game; compute in dbt)."""
    log.info("Loading player season totals for season %s...", season)
    df = player_season_totals(season=season)
    if df.empty:
        log.warning("No player totals returned")
        return df
    df = normalize_columns(df)
    df = ensure_season_column(df, season)
    log.info("  player_season_totals: %d rows", len(df))
    return df


def load_rosters(season: str) -> pd.DataFrame:
    """Load roster for all 30 teams; one row per player-team-season."""
    log.info("Loading rosters for season %s (%d teams)...", season, len(NBA_TEAM_ABBREVS))
    frames = []
    for i, abbrev in enumerate(NBA_TEAM_ABBREVS, 1):
        df = team_roster(team_abbrev=abbrev, season=season)
        if df.empty:
            log.debug("  %s: no roster", abbrev)
            continue
        df = normalize_columns(df)
        df = df.assign(team_abbrev=abbrev, season=season)
        frames.append(df)
        log.info("  %s: %d players (%d/%d)", abbrev, len(df), i, len(NBA_TEAM_ABBREVS))
    if not frames:
        log.warning("No roster data returned")
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("  roster total: %d rows", len(out))
    return out


def load_all_raw(season: str) -> dict[str, pd.DataFrame]:
    """Scrape and return raw tables as DataFrames."""
    log.info("Fetching raw data for season %s", season)
    tables = {
        "games": load_games(season),
        "player_season_totals": load_player_totals(season),
        "roster": load_rosters(season),
    }
    log.info("Raw fetch complete: games=%d, player_season_totals=%d, roster=%d",
             len(tables["games"]), len(tables["player_season_totals"]), len(tables["roster"]))
    return tables


def write_duckdb(tables: dict[str, pd.DataFrame], db_path: str) -> None:
    """Write raw tables to DuckDB (replace if exists for idempotency)."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Writing to DuckDB: %s", path)
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    for name, df in tables.items():
        if df.empty:
            log.debug("Skipping empty table: %s", name)
            continue
        con.execute(f"DROP TABLE IF EXISTS raw.{name}")
    for name, df in tables.items():
        if df.empty:
            continue
        con.register("_df", df)
        con.execute(f"CREATE TABLE raw.{name} AS SELECT * FROM _df")
        con.unregister("_df")
        log.info("  raw.%s: %d rows", name, len(df))
    con.close()
    log.info("DuckDB write complete")


def export_to_s3(db_path: str, bucket: str, prefix: str) -> None:
    """Export raw tables from DuckDB to S3 as Parquet (requires httpfs)."""
    log.info("Exporting to S3 (bucket=%s, prefix=%s)...", bucket, prefix)
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Use default AWS credential chain (env vars or ~/.aws/credentials)
    con.execute("SET s3_region = 'us-east-1';")  # override via env if needed
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchall()
    for (table_name,) in tables:
        s3_path = f"s3://{bucket}/{prefix.rstrip('/')}/raw/{table_name}.parquet"
        log.info("  %s -> %s", table_name, s3_path)
        con.execute(f"COPY raw.{table_name} TO '{s3_path}' (FORMAT PARQUET)")
    con.close()
    log.info("S3 export complete")


def main() -> int:
    parser = argparse.ArgumentParser(description="Load raw NBA data into DuckDB (+ optional S3)")
    parser.add_argument("--season", default="2026", help="Season year (e.g. 2026 for 2025-26)")
    parser.add_argument("--db", default="warehouse.duckdb", help="Path to DuckDB file")
    parser.add_argument("--s3-bucket", default=os.environ.get("NBA_S3_BUCKET"), help="S3 bucket (or NBA_S3_BUCKET)")
    parser.add_argument("--s3-prefix", default=os.environ.get("NBA_S3_PREFIX", "nba"), help="S3 key prefix")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    season = args.season
    log.info("Starting ingest for season %s", season)
    tables = load_all_raw(season)
    write_duckdb(tables, args.db)
    if args.s3_bucket:
        export_to_s3(args.db, args.s3_bucket, args.s3_prefix)
    else:
        log.info("Skipping S3 (set --s3-bucket or NBA_S3_BUCKET to export)")
    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
