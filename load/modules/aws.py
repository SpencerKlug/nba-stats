"""AWS/S3 export: write raw DuckDB tables to S3 as Parquet."""

from __future__ import annotations

import logging

import duckdb

log = logging.getLogger(__name__)


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
