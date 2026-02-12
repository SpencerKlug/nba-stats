# NBA Stats: Scrape → DuckDB → dbt

Raw NBA data from the `stats.nba.com` API is ingested into a **DuckDB** warehouse (with optional **AWS S3**), then transformed in **dbt**. No pre-aggregated marts are stored in ingest; standings and per-game stats are built in dbt from raw game logs.

## Architecture

- **Ingest**: Python pulls raw API tables (`leaguegamelog`, `commonteamroster`) and loads them into DuckDB. Optionally exports to S3 as Parquet.
- **Warehouse**: DuckDB (local `warehouse.duckdb` or query over S3).
- **dbt**: Staging models clean raw data; marts build standings (from team game logs) and player per-game stats (from player game logs).

## Setup

### 1. Python (scraper + ingest)

```bash
pip install -r requirements.txt
```

### 2. Ingest raw data into DuckDB

From the project root:

```bash
# Default: season 2026 (2025-26), writes to warehouse.duckdb
python -m ingest.load_warehouse --season 2026

# Custom DB path
python -m ingest.load_warehouse --season 2026 --db path/to/warehouse.duckdb

# Export to S3 (uses AWS credentials from env or ~/.aws/credentials)
export NBA_S3_BUCKET=your-bucket
export AWS_ACCESS_KEY_ID=...   # if not using default profile
export AWS_SECRET_ACCESS_KEY=...
python -m ingest.load_warehouse --season 2026 --s3-bucket your-bucket --s3-prefix nba/raw
```

Ingest writes three raw tables under the `raw` schema:

- **team_game_logs** – one row per team per game (`leaguegamelog`, `PlayerOrTeam=T`).
- **player_game_logs** – one row per player per game (`leaguegamelog`, `PlayerOrTeam=P`).
- **team_rosters** – one row per player-team-season (`commonteamroster`).

### 3. dbt (transformations)

Install dbt with the DuckDB adapter:

```bash
pip install dbt-duckdb
```

Run dbt from the project root with the profile in the `dbt` folder:

```bash
cd /path/to/nba-stats
DBT_PROFILES_DIR=dbt dbt deps
DBT_PROFILES_DIR=dbt dbt run
```

By default, the profile uses `path: warehouse.duckdb` (relative to the current working directory). Point it at your DB path in `dbt/profiles.yml` if needed.

**Models:**

- **Staging**: `stg_games`, `stg_player_totals`, `stg_roster` – cleanup and column alignment from raw API tables.
- **Marts**:
  - **standings** – conference standings (W, L, W/L%, GB) derived from `team_game_logs` only.
  - **player_per_game** – per-game stats (PPG, RPG, APG, …) derived from `player_game_logs`.

### 4. Querying DuckDB

```bash
duckdb warehouse.duckdb
```

```sql
SELECT * FROM staging.stg_games LIMIT 5;
SELECT * FROM marts.standings ORDER BY conference, conf_rank;
SELECT * FROM marts.player_per_game WHERE season = '2026' ORDER BY pts DESC LIMIT 10;
```

## Saving data in AWS

- **Option A – Ingest exports to S3**: Use `--s3-bucket` and `--s3-prefix` (or `NBA_S3_BUCKET` / `NBA_S3_PREFIX`). Ingest writes Parquet files to `s3://<bucket>/<prefix>/raw/<table>.parquet`. You can then attach S3 in DuckDB and run dbt against those paths (e.g. by changing the DuckDB path in the profile to a DB that reads from S3).
- **Option B – Local DuckDB only**: Keep `warehouse.duckdb` local and back it up or sync to S3 yourself.

## Project layout

```
nba-stats/
├── main.py                  # optional playground for API calls
├── ingest/
│   └── load_warehouse.py      # stats.nba.com API -> DuckDB (+ optional S3)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml          # DuckDB path; use DBT_PROFILES_DIR=dbt
│   ├── packages.yml
│   └── models/
│       ├── sources.yml       # raw.team_game_logs, raw.player_game_logs, raw.team_rosters
│       ├── staging/
│       └── marts/
├── warehouse.duckdb          # Created by ingest (path configurable)
└── requirements.txt
```

## Notes

- Ingest uses conservative headers + backoff for `stats.nba.com`.
- Raw data only: standings and per-game views are built in dbt from raw logs.
