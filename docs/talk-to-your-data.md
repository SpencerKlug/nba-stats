# Talk to Your Data with ChatGPT and MCP

This project includes a small **MCP (Model Context Protocol) server** that exposes your NBA stats DuckDB warehouse as tools. You can connect it to **ChatGPT** (the product) so you can ask questions in natural language and get answers from your data.

## Quick start (how to get started)

1. **Have data** – From the repo root, load and transform so the warehouse exists:
   ```bash
   python -m load.load_warehouse --season 2026
   DBT_PROFILES_DIR=transform dbt run --project-dir transform
   ```

2. **Install MCP deps** – In the same repo:
   ```bash
   pip install -r requirements.txt -r requirements-mcp.txt
   ```

3. **Run the MCP server** – From the repo root:
   ```bash
   python -m mcp_server
   ```
   Server is at **http://localhost:8000/mcp**.

4. **Use with ChatGPT** – Expose the server with [ngrok](https://ngrok.com/) (`ngrok http 8000`), then in ChatGPT: **Settings → Connectors → Create** and set the Connector URL to **https://YOUR_NGROK_HOST/mcp**. In a new chat, enable Developer mode and turn on your connector, then ask e.g. “Who are the top 5 scorers in 2026?”

Details for each step are below.

---

## How it works

1. You run the MCP server (optionally behind [ngrok](https://ngrok.com/) for a public URL).
2. You register the server URL as a **Connector** in ChatGPT and enable it in a chat.
3. ChatGPT calls the server’s tools (`list_tables`, `run_sql`) to answer questions like “Who are the top 5 scorers this season?”

## Prerequisites

- A populated warehouse: run [load](../README.md#2-load-raw-data-into-duckdb) and [dbt](../README.md#3-transform-dbt) so `warehouse.duckdb` (or your DB path) has data.
- Python 3.10+ with the project’s base deps and MCP deps installed.

## 1. Install dependencies

From the project root:

```bash
pip install -r requirements.txt -r requirements-mcp.txt
```

## 2. Run the MCP server

From the **project root** (so the default DB path `warehouse.duckdb` resolves correctly):

```bash
python -m mcp_server
```

The server listens on **http://0.0.0.0:8000** with the **Streamable HTTP** transport at path **/mcp**. So the base URL for the MCP endpoint is:

- **http://localhost:8000/mcp** (local)
- For ChatGPT you need **HTTPS** and a public URL (see step 3).

Optional env:

- **DUCKDB_PATH** – Path to your DuckDB file (default: `warehouse.duckdb`, relative to current working directory).

Example with a custom DB path:

```bash
export DUCKDB_PATH=/path/to/warehouse.duckdb
python -m mcp_server
```

## 3. Expose with ngrok (for ChatGPT)

ChatGPT can only call **remote** MCP servers (public HTTPS URL). For a quick start:

1. Install [ngrok](https://ngrok.com/download).
2. With the MCP server running on port 8000, in another terminal run:

   ```bash
   ngrok http 8000
   ```

3. Copy the **HTTPS** URL ngrok shows (e.g. `https://abc123.ngrok-free.app`). The full MCP endpoint for ChatGPT is:

   **https://YOUR_NGROK_HOST/mcp**

   Example: `https://abc123.ngrok-free.app/mcp`

ChatGPT can reach your data only while your machine is on and both the server and ngrok are running.

## 4. Register the server in ChatGPT

1. In **ChatGPT**: go to **Settings → Apps & Connectors → Advanced** and turn **Developer Mode** ON.
2. Go to **Settings → Connectors → Create**.
3. Set:
   - **Connector URL**: your public MCP URL (e.g. `https://abc123.ngrok-free.app/mcp`).
   - **Name**: e.g. “NBA stats warehouse”.
   - **Description**: optional (e.g. “Query NBA stats DuckDB: list_tables, run_sql”).
   - **Authentication**: “No Authentication” for dev; use API key or similar in production if your setup supports it.
4. Save the connector.
5. In a **new chat**: click the “+” in the message composer → **More → Developer mode** → enable your NBA stats connector.
6. Ask in natural language, e.g.:
   - “Who are the top 5 scorers in 2026?”
   - “What’s in the marts schema?”
   - “List tables and columns I can query.”

ChatGPT will call `list_tables` and/or `run_sql` and answer from your warehouse.

## Tools exposed to ChatGPT

| Tool          | Description |
|---------------|-------------|
| **list_tables** | Lists tables and columns in schemas `raw`, `staging`, `intermediate`, `marts`. Use first to discover what to query. |
| **run_sql**    | Runs a **read-only SELECT** against the DuckDB warehouse. Results are limited (default 100 rows; max 500). Only `SELECT` is allowed. |

Security: the server rejects non-SELECT statements and uses a read-only DuckDB connection. No credentials are exposed; the DB path comes from `DUCKDB_PATH` (or default).

## Production (optional)

For a stable URL without ngrok:

- Deploy the MCP server to a host that runs Python (e.g. Fly.io, Railway, a small VM).
- Ensure the host can read the DuckDB file (e.g. sync `warehouse.duckdb` or attach S3-backed data).
- Serve over **HTTPS** (platform TLS or a reverse proxy).
- Register the deployed MCP URL (e.g. `https://nba-mcp.yourdomain.com/mcp`) as the Connector in ChatGPT.

## Project layout (MCP)

```
mcp_server/
├── __init__.py
├── __main__.py   # Entrypoint: python -m mcp_server
├── db.py         # DuckDB connection, run_read_only_query, list_tables_and_columns
├── server.py     # FastMCP app and tools: list_tables, run_sql
requirements-mcp.txt   # fastmcp (DuckDB is in requirements.txt)
docs/talk-to-your-data.md  # This file
```
