"""
Load raw NBA data from stats.nba.com into DuckDB (data warehouse).
Writes to a local DuckDB file and can export tables to S3 as Parquet.

Raw-only tables loaded:
- team_game_logs (one row per team per game)
- player_game_logs (one row per player per game)
- team_rosters (one row per player-team-season)

All aggregations (standings, per-game rollups, etc.) are done in dbt.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd
import requests

log = logging.getLogger(__name__)

STATS_BASE_URL = "https://stats.nba.com/stats"
STATS_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://stats.nba.com/",
    "Origin": "https://stats.nba.com",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = int(os.getenv("NBA_API_MAX_RETRIES", "7"))
REQUEST_DELAY_SECONDS = float(os.getenv("NBA_API_REQUEST_DELAY_SECONDS", "1.5"))
BACKOFF_INITIAL_SECONDS = float(os.getenv("NBA_API_BACKOFF_INITIAL_SECONDS", "2.0"))
BACKOFF_MAX_SECONDS = float(os.getenv("NBA_API_BACKOFF_MAX_SECONDS", "120.0"))

_SESSION = requests.Session()
_SESSION.headers.update(STATS_HEADERS)


def to_snake_case(s: str) -> str:
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", "_", s.strip()).lower()
    return s or "unknown"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame columns to snake_case; dedupe with _1, _2 suffix."""
    df = df.copy()
    base = [to_snake_case(str(c)) for c in df.columns]
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in base:
        if name in seen:
            seen[name] += 1
            out.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            out.append(name)
    df.columns = out
    return df


def season_to_label(season: str) -> str:
    """Convert season year to NBA API label. 2026 -> 2025-26"""
    y = int(season)
    return f"{y-1}-{str(y)[-2:]}"


def _retry_wait_seconds(attempt: int, resp: requests.Response | None = None) -> float:
    backoff = min(BACKOFF_INITIAL_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
    if resp is not None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), backoff), BACKOFF_MAX_SECONDS)
            except ValueError:
                pass
    return backoff


def call_stats_api(endpoint: str, params: dict[str, str]) -> dict:
    """Call a stats.nba.com endpoint with retries/backoff."""
    url = f"{STATS_BASE_URL}/{endpoint}"
    time.sleep(REQUEST_DELAY_SECONDS)
    for attempt in range(MAX_RETRIES + 1):
        log.info("GET %s attempt=%d/%d", endpoint, attempt + 1, MAX_RETRIES + 1)
        resp = _SESSION.get(url, params=params, timeout=30)
        if resp.status_code in RETRY_STATUS_CODES:
            if attempt < MAX_RETRIES:
                wait = _retry_wait_seconds(attempt, resp)
                log.warning("status=%s endpoint=%s retrying in %.1fs", resp.status_code, endpoint, wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed to fetch endpoint={endpoint}")


def resultset_to_df(payload: dict, name: str | None = None, index: int = 0) -> pd.DataFrame:
    """Parse NBA stats resultSet(s) JSON to DataFrame."""
    if "resultSets" in payload:
        sets = payload["resultSets"]
        if isinstance(sets, dict):
            headers = sets.get("headers", [])
            rows = sets.get("rowSet", [])
            return pd.DataFrame(rows, columns=headers)
        if name is not None:
            for rs in sets:
                if rs.get("name") == name:
                    return pd.DataFrame(rs.get("rowSet", []), columns=rs.get("headers", []))
        rs = sets[index]
        return pd.DataFrame(rs.get("rowSet", []), columns=rs.get("headers", []))

    if "resultSet" in payload:
        rs = payload["resultSet"]
        return pd.DataFrame(rs.get("rowSet", []), columns=rs.get("headers", []))

    return pd.DataFrame()


def load_team_game_logs(season: str, season_type: str = "Regular Season") -> pd.DataFrame:
    season_label = season_to_label(season)
    log.info("Loading team game logs season=%s (%s)", season_label, season_type)
    payload = call_stats_api(
        "leaguegamelog",
        {
            "Counter": "1000",
            "Direction": "DESC",
            "LeagueID": "00",
            "PlayerOrTeam": "T",
            "Season": season_label,
            "SeasonType": season_type,
            "Sorter": "DATE",
        },
    )
    df = resultset_to_df(payload)
    if df.empty:
        log.warning("No team game logs returned")
        return df
    df = normalize_columns(df)
    df["season"] = season
    df["season_label"] = season_label
    df["season_type"] = season_type
    log.info("  team_game_logs: %d rows", len(df))
    return df


def load_player_game_logs(season: str, season_type: str = "Regular Season") -> pd.DataFrame:
    season_label = season_to_label(season)
    log.info("Loading player game logs season=%s (%s)", season_label, season_type)
    payload = call_stats_api(
        "leaguegamelog",
        {
            "Counter": "1000",
            "Direction": "DESC",
            "LeagueID": "00",
            "PlayerOrTeam": "P",
            "Season": season_label,
            "SeasonType": season_type,
            "Sorter": "DATE",
        },
    )
    df = resultset_to_df(payload)
    if df.empty:
        log.warning("No player game logs returned")
        return df
    df = normalize_columns(df)
    df["season"] = season
    df["season_label"] = season_label
    df["season_type"] = season_type
    log.info("  player_game_logs: %d rows", len(df))
    return df


def load_team_rosters(season: str, team_game_logs: pd.DataFrame) -> pd.DataFrame:
    """Load commonteamroster for each team id observed in team_game_logs."""
    if team_game_logs.empty or "team_id" not in team_game_logs.columns:
        log.warning("Cannot load rosters: team_game_logs empty or missing team_id")
        return pd.DataFrame()

    season_label = season_to_label(season)
    teams = (
        team_game_logs[["team_id", "team_abbreviation"]]
        .drop_duplicates()
        .sort_values("team_abbreviation")
    )

    frames: list[pd.DataFrame] = []
    log.info("Loading rosters for %d teams", len(teams))

    for i, row in enumerate(teams.itertuples(index=False), 1):
        team_id = int(row.team_id)
        team_abbrev = str(row.team_abbreviation)
        payload = call_stats_api(
            "commonteamroster",
            {"LeagueID": "00", "Season": season_label, "TeamID": str(team_id)},
        )
        df = resultset_to_df(payload, name="CommonTeamRoster")
        if df.empty:
            log.warning("  %s (%s): empty roster", team_abbrev, team_id)
            continue
        df = normalize_columns(df)
        df["team_id"] = team_id
        df["team_abbreviation"] = team_abbrev
        df["season"] = season
        df["season_label"] = season_label
        frames.append(df)
        log.info("  %s (%s): %d players (%d/%d)", team_abbrev, team_id, len(df), i, len(teams))

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("  team_rosters: %d rows", len(out))
    return out


def load_all_raw(season: str, season_type: str = "Regular Season") -> dict[str, pd.DataFrame]:
    """Fetch all raw tables from NBA stats API."""
    log.info("Fetching raw NBA stats data for season=%s season_type=%s", season, season_type)
    team_logs = load_team_game_logs(season=season, season_type=season_type)
    player_logs = load_player_game_logs(season=season, season_type=season_type)
    rosters = load_team_rosters(season=season, team_game_logs=team_logs)

    tables = {
        "team_game_logs": team_logs,
        "player_game_logs": player_logs,
        "team_rosters": rosters,
    }
    log.info(
        "Raw fetch complete: team_game_logs=%d, player_game_logs=%d, team_rosters=%d",
        len(team_logs),
        len(player_logs),
        len(rosters),
    )
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
    con.execute("SET s3_region = 'us-east-1';")
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

    log.info("Starting ingest for season=%s season_type=%s", args.season, args.season_type)
    tables = load_all_raw(season=args.season, season_type=args.season_type)
    write_duckdb(tables, args.db)

    if args.s3_bucket:
        export_to_s3(args.db, args.s3_bucket, args.s3_prefix)
    else:
        log.info("Skipping S3 (set --s3-bucket or NBA_S3_BUCKET to export)")

    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
