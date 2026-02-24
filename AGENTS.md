# AGENTS.md

## Cursor Cloud specific instructions

### Project overview
NBA Stats ELT pipeline: Python loads raw data from `stats.nba.com` API into DuckDB, then dbt transforms it into analytics marts. See `README.md` for full architecture and commands.

### Services
| Service | How to run | Notes |
|---|---|---|
| **Load (Python)** | `python3 -m load.load_warehouse --season 2026` | Requires internet access to `stats.nba.com`; use `--limit N` for test mode |
| **Transform (dbt)** | `DBT_PROFILES_DIR=transform dbt run --project-dir transform` | Requires `warehouse.duckdb` with raw data; run `dbt deps` first |
| **Ask (Q&A agent)** | `python3 -m ask "Who leads in scoring?"` | Requires `OPENAI_API_KEY`; omit question arg for interactive REPL |

### Key caveats
- The `stats.nba.com` API is rate-limited and may timeout from cloud/CI environments. Use `--limit 2` for minimal test runs. If the API is unreachable, you can seed mock data directly into DuckDB under the `raw` schema (tables: `team_game_logs`, `player_game_logs`, `team_rosters`, `common_all_players`, `player_info`, `schedule`, `box_summaries`).
- `dbt-duckdb` is not listed in `requirements.txt` but is required at runtime. The update script installs it separately.
- `~/.local/bin` must be on `PATH` for `dbt` and `ruff` CLI commands (pip installs scripts there when not using a virtualenv).
- Lint: `ruff check .` and `ruff format --check .` (config in `pyproject.toml`).
- dbt profiles live in `transform/profiles.yml`; always set `DBT_PROFILES_DIR=transform` when running dbt commands from the project root.
- The DuckDB file (`warehouse.duckdb`) is created at the project root by default. Close other DuckDB clients before writing.
- `python` is not aliased; use `python3` explicitly.
