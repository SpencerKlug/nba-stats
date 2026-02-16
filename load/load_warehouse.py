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
    """Convert a string to snake_case (e.g. 'W/L%' -> 'w_l_pct').

    Args:
        s (str): Input string to normalize.

    Returns:
        str: Snake_case string.
    """
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", "_", s.strip()).lower()
    return s or "unknown"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame columns to snake_case; dedupe with _1, _2 suffix.

    Args:
        df (pd.DataFrame): DataFrame whose columns to normalize.

    Returns:
        pd.DataFrame: DataFrame with normalized column names.
    """
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
    """Convert season year to NBA API label (e.g. 2026 -> 2025-26).

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).

    Returns:
        str: NBA API season label (e.g. 2025-26).
    """
    y = int(season)
    return f"{y - 1}-{str(y)[-2:]}"


def _retry_wait_seconds(attempt: int, resp: requests.Response | None = None) -> float:
    """Compute retry wait time (exponential backoff, optional Retry-After header).

    Args:
        attempt (int): Current attempt index (0-based).
        resp (requests.Response | None, optional): Response from failed request (for Retry-After). Defaults to None.

    Returns:
        float: Seconds to wait before retry.
    """
    backoff = min(BACKOFF_INITIAL_SECONDS * (2**attempt), BACKOFF_MAX_SECONDS)
    if resp is not None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), backoff), BACKOFF_MAX_SECONDS)
            except ValueError:
                pass
    return backoff


def call_stats_api(endpoint: str, params: dict[str, str]) -> dict:
    """Call a stats.nba.com endpoint with retries and exponential backoff.

    Args:
        endpoint (str): API endpoint path (e.g. leaguegamelog).
        params (dict[str, str]): Query parameters for the request.

    Returns:
        dict: JSON response body.
    """
    url = f"{STATS_BASE_URL}/{endpoint}"
    time.sleep(REQUEST_DELAY_SECONDS)
    for attempt in range(MAX_RETRIES + 1):
        log.info("GET %s attempt=%d/%d", endpoint, attempt + 1, MAX_RETRIES + 1)
        resp = _SESSION.get(url, params=params, timeout=30)
        if resp.status_code in RETRY_STATUS_CODES:
            if attempt < MAX_RETRIES:
                wait = _retry_wait_seconds(attempt, resp)
                log.warning(
                    "status=%s endpoint=%s retrying in %.1fs",
                    resp.status_code,
                    endpoint,
                    wait,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed to fetch endpoint={endpoint}")


def resultset_to_df(payload: dict, name: str | None = None, index: int = 0) -> pd.DataFrame:
    """Parse NBA stats API resultSet(s) JSON into a DataFrame.

    Args:
        payload (dict): API response JSON.
        name (str | None, optional): Result set name to select. Defaults to None.
        index (int, optional): Index of result set when name not used. Defaults to 0.

    Returns:
        pd.DataFrame: Parsed table; empty if no matching result set.
    """
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
    """Load team game logs from stats.nba.com API.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26)
        season_type (str, optional): NBA API season type. Defaults to "Regular Season".

    Returns:
        pd.DataFrame: Team game logs DataFrame
    """
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
    """Load player game logs from stats.nba.com API.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).
        season_type (str, optional): NBA API season type. Defaults to "Regular Season".

    Returns:
        pd.DataFrame: Player game logs DataFrame.
    """
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
    """Load commonteamroster for each team id observed in team game logs.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).
        team_game_logs (pd.DataFrame): Team game logs used to derive team IDs.

    Returns:
        pd.DataFrame: Roster rows (one per player-team-season); empty if no teams.
    """
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
        log.info(
            "  %s (%s): %d players (%d/%d)",
            team_abbrev,
            team_id,
            len(df),
            i,
            len(teams),
        )

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("  team_rosters: %d rows", len(out))
    return out


def load_all_raw(season: str, season_type: str = "Regular Season") -> dict[str, pd.DataFrame]:
    """Fetch all raw tables (team logs, player logs, rosters) from NBA stats API.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).
        season_type (str, optional): NBA API season type. Defaults to "Regular Season".

    Returns:
        dict[str, pd.DataFrame]: Keys team_game_logs, player_game_logs, team_rosters.
    """
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


def init_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB connection and ensure raw schema exists.

    Args:
        db_path (str): Path to DuckDB file.

    Returns:
        duckdb.DuckDBPyConnection: Open connection.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Opening DuckDB: %s", path)
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    return con


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """Check whether a table exists in the given schema.

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection.
        schema (str): Schema name.
        table (str): Table name.

    Returns:
        bool: True if the table exists.
    """
    cnt = con.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()[0]
    return cnt > 0


def align_df_to_existing_columns(df: pd.DataFrame, existing_cols: list[str]) -> pd.DataFrame:
    """Align incoming DataFrame to existing table columns (add NULLs, drop extras).

    Args:
        df (pd.DataFrame): Incoming DataFrame.
        existing_cols (list[str]): Column names of the existing table.

    Returns:
        pd.DataFrame: DataFrame with only existing_cols, missing cols as NULL.
    """
    out = df.copy()
    for c in existing_cols:
        if c not in out.columns:
            out[c] = None
    extra = [c for c in out.columns if c not in existing_cols]
    if extra:
        log.warning("Dropping %d new/unexpected columns: %s", len(extra), ", ".join(extra))
    return out[existing_cols]


def upsert_raw_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    df: pd.DataFrame,
    season: str,
    season_type: str,
) -> None:
    """Create table or upsert season-level rows (idempotent for backfill reruns).

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection.
        table_name (str): Raw table name (e.g. team_game_logs).
        df (pd.DataFrame): Data to write.
        season (str): Season year (e.g. 2026).
        season_type (str): NBA API season type (e.g. Regular Season).

    Returns:
        None
    """
    if df.empty:
        log.debug("Skipping empty table: %s", table_name)
        return

    fq_table = f"raw.{table_name}"
    if not table_exists(con, "raw", table_name):
        con.register("_df", df)
        con.execute(f"CREATE TABLE {fq_table} AS SELECT * FROM _df")
        con.unregister("_df")
        log.info("  created %s: %d rows", fq_table, len(df))
        return

    existing_cols = [row[0] for row in con.execute(f"DESCRIBE {fq_table}").fetchall()]
    aligned = align_df_to_existing_columns(df, existing_cols)
    con.register("_df", aligned)

    # Idempotent season-level overwrite for reruns/backfills.
    if "season" in existing_cols and "season_type" in existing_cols:
        con.execute(
            f"DELETE FROM {fq_table} WHERE season = ? AND season_type = ?",
            [season, season_type],
        )
    elif "season" in existing_cols:
        con.execute(f"DELETE FROM {fq_table} WHERE season = ?", [season])

    cols_sql = ", ".join([f'"{c}"' for c in existing_cols])
    con.execute(f"INSERT INTO {fq_table} ({cols_sql}) SELECT {cols_sql} FROM _df")
    con.unregister("_df")
    log.info("  upserted %s: %d rows", fq_table, len(aligned))


def write_duckdb_for_season(
    con: duckdb.DuckDBPyConnection,
    tables: dict[str, pd.DataFrame],
    season: str,
    season_type: str,
) -> None:
    """Write one season's raw tables into DuckDB (upsert per table).

    Args:
        con (duckdb.DuckDBPyConnection): DuckDB connection.
        tables (dict[str, pd.DataFrame]): Raw tables (team_game_logs, player_game_logs, team_rosters).
        season (str): Season year (e.g. 2026).
        season_type (str): NBA API season type.

    Returns:
        None
    """
    log.info("Writing season=%s season_type=%s to DuckDB", season, season_type)
    for name, df in tables.items():
        upsert_raw_table(con, name, df, season=season, season_type=season_type)


def export_to_s3(db_path: str, bucket: str, prefix: str) -> None:
    """Export raw schema tables from DuckDB to S3 as Parquet (requires httpfs).

    Args:
        db_path (str): Path to DuckDB file.
        bucket (str): S3 bucket name.
        prefix (str): S3 key prefix (e.g. nba).

    Returns:
        None
    """
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


def resolve_seasons(
    season: str,
    start_season: str | None,
    end_season: str | None,
) -> list[str]:
    """Resolve season CLI inputs to a sorted inclusive list of season years.

    Args:
        season (str): Default season year (e.g. 2026).
        start_season (str | None): Backfill start year; with end_season gives range.
        end_season (str | None): Backfill end year; with start_season gives range.

    Returns:
        list[str]: Season years, e.g. ['2026'] or ['1997', ..., '2026'].
    """
    if start_season is None and end_season is None:
        return [str(int(season))]

    start = int(start_season or season)
    end = int(end_season or season)
    if start > end:
        raise ValueError(f"start-season ({start}) must be <= end-season ({end})")
    return [str(y) for y in range(start, end + 1)]


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

    seasons = resolve_seasons(args.season, args.start_season, args.end_season)
    log.info(
        "Starting load for %d season(s): %s -> %s (season_type=%s)",
        len(seasons),
        seasons[0],
        seasons[-1],
        args.season_type,
    )

    con = init_duckdb(args.db)
    try:
        for i, season in enumerate(seasons, 1):
            log.info("=== Season %s (%d/%d) ===", season, i, len(seasons))
            tables = load_all_raw(season=season, season_type=args.season_type)
            write_duckdb_for_season(con, tables, season=season, season_type=args.season_type)
    finally:
        con.close()
        log.info("DuckDB connection closed")

    if args.s3_bucket:
        export_to_s3(args.db, args.s3_bucket, args.s3_prefix)
    else:
        log.info("Skipping S3 (set --s3-bucket or NBA_S3_BUCKET to export)")

    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
