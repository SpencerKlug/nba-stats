"""DuckDB warehouse: three separate databases (bronze, silver, gold)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

import duckdb
import pandas as pd

from load.modules import utils

log = logging.getLogger(__name__)

# Bronze = raw data, one schema per source (nba, ncaa). Silver/gold = separate DB files.
BRONZE_SOURCES = ("nba", "ncaa")
Source = Literal["nba", "ncaa"]


def _bronze_path(db_path: str) -> Path:
    """Path to the bronze database file (raw data by source)."""
    p = Path(db_path)
    if p.suffix != ".duckdb":
        p = p / "bronze.duckdb"
    return p.parent / "bronze.duckdb" if p.name != "bronze.duckdb" else p


def _warehouse_dir(db_path: str) -> Path:
    """Directory containing bronze.duckdb, silver.duckdb, gold.duckdb."""
    return _bronze_path(db_path).parent


def _silver_path(db_path: str) -> Path:
    """Path to the silver database file (by domain)."""
    return _warehouse_dir(db_path) / "silver.duckdb"


def _gold_path(db_path: str) -> Path:
    """Path to the gold database file (aggregates)."""
    return _warehouse_dir(db_path) / "gold.duckdb"


def init_duckdb(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open the bronze database and ensure schemas nba, ncaa exist.

    Three separate database files live in the same directory as db_path:
    - bronze.duckdb: raw data with schemas nba, ncaa
    - silver.duckdb: curated by domain (created empty if missing)
    - gold.duckdb: aggregates (created empty if missing)

    Args:
        db_path: Path to the bronze database (e.g. bronze.duckdb) or any path
                 in the target directory; bronze/silver/gold will be used.

    Returns:
        Open connection to the bronze database.
    """
    path = _bronze_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("Opening bronze DuckDB: %s", path)
    con = duckdb.connect(str(path))
    for schema in BRONZE_SOURCES:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    # Ensure silver and gold DB files exist (empty) so all three DBs are present
    for name, fn in (("silver", _silver_path), ("gold", _gold_path)):
        p = fn(db_path)
        if not p.exists():
            c = duckdb.connect(str(p))
            c.execute("CREATE SCHEMA IF NOT EXISTS main")
            c.close()
            log.info("Created empty %s database: %s", name, p)
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
    """Create or upsert season-level rows in the bronze DB under schema nba or ncaa.

    Args:
        con: DuckDB connection (to bronze.duckdb).
        source: 'nba' or 'ncaa'; data is written to schema nba or ncaa.
        table_name: Table name (e.g. team_game_logs, ncaa_team_list).
        df: Data to write.
        season: Season year (e.g. 2026).
        season_type: NBA API season type (e.g. Regular Season). None for NCAA.
    """
    if df.empty:
        log.debug("Skipping empty table: %s.%s", source, table_name)
        return

    fq_table = f"{source}.{table_name}"
    if not table_exists(con, source, table_name):
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
    """Write one season's raw tables into the bronze database (schema nba or ncaa).

    Args:
        con: DuckDB connection (bronze.duckdb).
        tables: Raw tables keyed by name (e.g. team_game_logs, ncaa_team_list).
        season: Season year (e.g. 2026).
        source: 'nba' or 'ncaa'; data is written to that schema in the bronze DB.
        season_type: NBA API season type. Pass None for NCAA.
    """
    log.info("Writing season=%s source=%s season_type=%s to bronze DuckDB", season, source, season_type)
    for name, df in tables.items():
        upsert_bronze_table(con, source, name, df, season=season, season_type=season_type)
