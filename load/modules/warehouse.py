"""DuckDB warehouse: bronze (raw by source), silver (by domain), gold (aggregates)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd

from load.modules import utils

log = logging.getLogger(__name__)

# Bronze = raw data, one schema per source. Silver = by domain. Gold = aggregates.
BRONZE_SOURCES = ("nba", "ncaa")
Source = Literal["nba", "ncaa"]


def init_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB and create medallion schemas.

    - bronze_nba, bronze_ncaa: raw data by source system
    - silver: curated data by domain (teams, games, etc.)
    - gold: aggregates and pre-defined metrics

    Args:
        db_path (str): Path to DuckDB file.

    Returns:
        duckdb.DuckDBPyConnection: Open connection.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Opening DuckDB: %s", path)
    con = duckdb.connect(str(path))
    for schema in ("bronze_nba", "bronze_ncaa", "silver", "gold"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return con


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    """Check whether a table exists in the given schema."""
    cnt = con.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()[0]
    return cnt > 0


def upsert_bronze_table(
    con: duckdb.DuckDBPyConnection,
    source: Source,
    table_name: str,
    df: pd.DataFrame,
    season: str,
    season_type: str | None = None,
) -> None:
    """Create or upsert season-level rows in the bronze schema for the given source.

    Args:
        con: DuckDB connection.
        source: Source system ('nba' or 'ncaa'); determines schema bronze_nba or bronze_ncaa.
        table_name: Table name (e.g. team_game_logs, ncaa_team_list).
        df: Data to write.
        season: Season year (e.g. 2026).
        season_type: NBA API season type (e.g. Regular Season). None for NCAA.
    """
    if df.empty:
        log.debug("Skipping empty table: %s.%s", source, table_name)
        return

    schema = f"bronze_{source}"
    fq_table = f"{schema}.{table_name}"
    if not table_exists(con, schema, table_name):
        con.register("_df", df)
        con.execute(f"CREATE TABLE {fq_table} AS SELECT * FROM _df")
        con.unregister("_df")
        log.info("  created %s: %d rows", fq_table, len(df))
        return

    existing_cols = [row[0] for row in con.execute(f"DESCRIBE {fq_table}").fetchall()]
    aligned = utils.align_df_to_existing_columns(df, existing_cols)
    con.register("_df", aligned)

    if "season" in existing_cols and "season_type" in existing_cols and season_type is not None:
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
    *,
    source: Source,
    season_type: str | None = None,
) -> None:
    """Write one season's raw tables into the bronze layer (schema by source).

    Args:
        con: DuckDB connection.
        tables: Raw tables keyed by name (e.g. team_game_logs, ncaa_team_list).
        season: Season year (e.g. 2026).
        source: 'nba' or 'ncaa'; data is written to bronze_nba or bronze_ncaa.
        season_type: NBA API season type. Pass None for NCAA.
    """
    log.info("Writing season=%s source=%s season_type=%s to DuckDB", season, source, season_type)
    for name, df in tables.items():
        upsert_bronze_table(con, source, name, df, season=season, season_type=season_type)
