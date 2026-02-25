"""FastMCP server exposing run_sql and list_tables for the NBA warehouse."""

from __future__ import annotations

from fastmcp import FastMCP

from mcp_server.db import DEFAULT_ROW_LIMIT, list_tables_and_columns, run_read_only_query

mcp = FastMCP(name="nba-stats-warehouse")


@mcp.tool()
def list_tables() -> str:
    """List available tables and columns in the warehouse (schemas: raw, staging, intermediate, marts). Use this to discover what you can query before calling run_sql."""
    return list_tables_and_columns()


@mcp.tool()
def run_sql(sql: str, max_rows: int = DEFAULT_ROW_LIMIT) -> str:
    """Execute a read-only SELECT query against the NBA stats DuckDB warehouse. Only SELECT is allowed. Results are limited to max_rows (default 100). Use list_tables first to see available tables and columns."""
    if max_rows < 1 or max_rows > 500:
        max_rows = DEFAULT_ROW_LIMIT
    return run_read_only_query(sql, max_rows=max_rows)
