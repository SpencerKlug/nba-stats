"""DuckDB connection and read-only query helpers for the MCP server."""

from __future__ import annotations

import os
import re
from collections import defaultdict

import duckdb

# Medallion: bronze by source, silver by domain, gold aggregates
ALLOWED_SCHEMAS = frozenset({"bronze_nba", "bronze_ncaa", "silver", "gold"})

DEFAULT_ROW_LIMIT = 100


def get_duckdb_path() -> str:
    """Return DuckDB path from DUCKDB_PATH env or default warehouse.duckdb (repo root)."""
    path = os.environ.get("DUCKDB_PATH", "warehouse.duckdb")
    if not os.path.isabs(path):
        # Resolve relative to cwd (assume run from repo root)
        path = os.path.abspath(path)
    return path


def connect() -> duckdb.DuckDBPyConnection:
    """Open a read-only connection to the warehouse DuckDB."""
    path = get_duckdb_path()
    conn = duckdb.connect(path, read_only=True)
    return conn


def validate_select_only(sql: str) -> None:
    """Raise ValueError if the SQL is not a single read-only SELECT."""
    stripped = sql.strip()
    # Allow only SELECT (and WITH ... SELECT); reject INSERT/UPDATE/DELETE/ etc.
    if not re.match(r"^(\s*WITH\s+.+\s+)?SELECT\s", stripped, re.IGNORECASE | re.DOTALL):
        raise ValueError("Only SELECT queries are allowed.")
    upper = stripped.upper()
    for verb in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"):
        if verb in upper:
            raise ValueError(f"Query must be read-only; found '{verb}'.")
    # Optional: ensure only allowed schemas are referenced
    for match in re.finditer(r"FROM\s+(\w+)\.(\w+)|JOIN\s+(\w+)\.(\w+)", stripped, re.IGNORECASE):
        schema = (match.group(1) or match.group(3) or "").strip()
        if schema and schema.lower() not in ALLOWED_SCHEMAS:
            raise ValueError(
                f"Schema '{schema}' is not allowed. Use one of: {sorted(ALLOWED_SCHEMAS)}."
            )
    # Also allow unqualified table names (DuckDB will use default schema)
    if re.search(r"\bFROM\s+(\w+)\b", stripped, re.IGNORECASE) and "." not in stripped:
        pass  # single table no schema - allow


def apply_limit(sql: str, max_rows: int = DEFAULT_ROW_LIMIT) -> str:
    """Append LIMIT to the query if not already present."""
    upper = sql.strip().upper()
    if "LIMIT" in upper:
        return sql
    return sql.rstrip().rstrip(";") + f" LIMIT {max_rows}"


def run_read_only_query(sql: str, max_rows: int = DEFAULT_ROW_LIMIT) -> str:
    """
    Execute a read-only SELECT against the warehouse and return results as text.
    Enforces SELECT-only, allowed schemas, and row limit.
    """
    validate_select_only(sql)
    limited = apply_limit(sql, max_rows)
    conn = connect()
    try:
        result = conn.execute(limited)
        rows = result.fetchall()
        columns = [d[0] for d in result.description]
        # Format as a simple table (markdown or CSV-like for model consumption)
        if not rows:
            return "No rows returned."
        lines = [
            "| " + " | ".join(columns) + " |",
            "| " + " | ".join("---" for _ in columns) + " |",
        ]
        for row in rows:
            lines.append("| " + " | ".join(str(v) if v is not None else "" for v in row) + " |")
        return "\n".join(lines)
    finally:
        conn.close()


def list_tables_and_columns() -> str:
    """Return a description of available schemas and tables for the LLM."""
    conn = connect()
    try:
        # Query information_schema for tables in allowed schemas
        out = conn.execute(
            """
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema IN ('bronze_nba','bronze_ncaa','silver','gold')
            ORDER BY table_schema, table_name, ordinal_position
            """
        ).fetchall()
        if not out:
            return "No tables found in schemas: bronze_nba, bronze_ncaa, silver, gold."
        # Group by schema.table
        by_table: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        for schema, table, col, dtype in out:
            by_table[(schema, table)].append((col, dtype))
        lines = [
            "Available tables (schema.table) and columns. Use these in run_sql SELECT queries.",
            "",
        ]
        for (schema, table), cols in sorted(by_table.items()):
            col_list = ", ".join(f"{c} ({t})" for c, t in cols)
            lines.append(f"- **{schema}.{table}**: {col_list}")
        return "\n".join(lines)
    finally:
        conn.close()
