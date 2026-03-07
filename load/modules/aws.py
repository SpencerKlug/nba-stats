"""AWS/S3 export: write bronze (raw) DuckDB tables to S3 as Parquet."""

from __future__ import annotations

import logging

import duckdb

log = logging.getLogger(__name__)

BRONZE_SCHEMAS = ("nba", "ncaa")


def export_to_s3(db_path: str, bucket: str, prefix: str) -> None:
    """Export bronze database (schemas nba, ncaa) to S3 as Parquet (requires httpfs).

    Each source schema is exported under prefix/nba/ and prefix/ncaa/
    with one Parquet file per table.

    Args:
        db_path: Path to DuckDB file.
        bucket: S3 bucket name.
        prefix: S3 key prefix (e.g. nba or warehouse).

    Returns:
        None
    """
    log.info("Exporting to S3 (bucket=%s, prefix=%s)...", bucket, prefix)
    from load.modules.warehouse import _bronze_path
    bronze_path = _bronze_path(db_path)
    con = duckdb.connect(str(bronze_path))
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region = 'us-east-1';")
    base = prefix.rstrip("/")
    for schema in BRONZE_SCHEMAS:
        tables = con.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = ?
            """,
            [schema],
        ).fetchall()
        for (table_name,) in tables:
            s3_path = f"s3://{bucket}/{base}/{schema}/{table_name}.parquet"
            log.info("  %s.%s -> %s", schema, table_name, s3_path)
            con.execute(f"COPY {schema}.{table_name} TO '{s3_path}' (FORMAT PARQUET)")
    con.close()
    log.info("S3 export complete")
