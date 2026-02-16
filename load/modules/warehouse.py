"""DuckDB warehouse: init, schema checks, and season-level upserts."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

from load.modules import utils

log = logging.getLogger(__name__)


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
    aligned = utils.align_df_to_existing_columns(df, existing_cols)
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
