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
import os
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

# Add project root for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from basketball_reference import (
    NBA_TEAM_ABBREVS,
    get_page,
    player_season_totals,
    schedule_results,
    team_roster,
)


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
    frames = []
    for month in SCHEDULE_MONTHS:
        df = schedule_results(season=season, month=month)
        if df.empty:
            continue
        df = normalize_columns(df)
        df = ensure_season_column(df, season)
        frames.append(df)
    if not frames:
        # Fallback: try main schedule page (may only have one month)
        df = schedule_results(season=season)
        if not df.empty:
            df = normalize_columns(df)
            df = ensure_season_column(df, season)
            return df
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    # Dedupe by date + visitor + home in case of overlap
    key = ["date", "visitor_neutral", "home_neutral"] if "visitor_neutral" in out.columns else ["date"]
    if all(c in out.columns for c in key):
        out = out.drop_duplicates(subset=key, keep="first")
    return out


def load_player_totals(season: str) -> pd.DataFrame:
    """Load raw player season totals (no per-game; compute in dbt)."""
    df = player_season_totals(season=season)
    if df.empty:
        return df
    df = normalize_columns(df)
    df = ensure_season_column(df, season)
    return df


def load_rosters(season: str) -> pd.DataFrame:
    """Load roster for all 30 teams; one row per player-team-season."""
    frames = []
    for abbrev in NBA_TEAM_ABBREVS:
        df = team_roster(team_abbrev=abbrev, season=season)
        if df.empty:
            continue
        df = normalize_columns(df)
        df = df.assign(team_abbrev=abbrev, season=season)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_all_raw(season: str) -> dict[str, pd.DataFrame]:
    """Scrape and return raw tables as DataFrames."""
    return {
        "games": load_games(season),
        "player_season_totals": load_player_totals(season),
        "roster": load_rosters(season),
    }


def write_duckdb(tables: dict[str, pd.DataFrame], db_path: str) -> None:
    """Write raw tables to DuckDB (replace if exists for idempotency)."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    for name, df in tables.items():
        if df.empty:
            continue
        con.execute(f"DROP TABLE IF EXISTS raw.{name}")
    for name, df in tables.items():
        if df.empty:
            continue
        con.register("_df", df)
        con.execute(f"CREATE TABLE raw.{name} AS SELECT * FROM _df")
        con.unregister("_df")
    con.close()


def export_to_s3(db_path: str, bucket: str, prefix: str) -> None:
    """Export raw tables from DuckDB to S3 as Parquet (requires httpfs)."""
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Use default AWS credential chain (env vars or ~/.aws/credentials)
    con.execute("SET s3_region = 'us-east-1';")  # override via env if needed
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchall()
    for (table_name,) in tables:
        s3_path = f"s3://{bucket}/{prefix.rstrip('/')}/raw/{table_name}.parquet"
        con.execute(f"COPY raw.{table_name} TO '{s3_path}' (FORMAT PARQUET)")
    con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Load raw NBA data into DuckDB (+ optional S3)")
    parser.add_argument("--season", default="2026", help="Season year (e.g. 2026 for 2025-26)")
    parser.add_argument("--db", default="warehouse.duckdb", help="Path to DuckDB file")
    parser.add_argument("--s3-bucket", default=os.environ.get("NBA_S3_BUCKET"), help="S3 bucket (or NBA_S3_BUCKET)")
    parser.add_argument("--s3-prefix", default=os.environ.get("NBA_S3_PREFIX", "nba"), help="S3 key prefix")
    args = parser.parse_args()

    season = args.season
    print(f"Loading raw data for season {season}...")
    tables = load_all_raw(season)
    for name, df in tables.items():
        print(f"  {name}: {len(df)} rows")
    print(f"Writing to {args.db}...")
    write_duckdb(tables, args.db)
    if args.s3_bucket:
        print(f"Exporting to s3://{args.s3_bucket}/{args.s3_prefix}...")
        export_to_s3(args.db, args.s3_bucket, args.s3_prefix)
    else:
        print("Skipping S3 (set --s3-bucket or NBA_S3_BUCKET to export).")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
