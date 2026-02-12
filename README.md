# NBA Stats: Scrape → DuckDB → dbt

Raw NBA data from [Basketball-Reference](https://www.basketball-reference.com/) is scraped into a **DuckDB** warehouse (with optional **AWS S3**), then transformed in **dbt**. No pre-aggregated stats are stored—standings and per-game stats are derived in dbt from raw games and player totals.

## Architecture

- **Ingest**: Python scrapes raw tables (games, player season totals, rosters) and loads them into DuckDB. Optionally exports to S3 as Parquet.
- **Warehouse**: DuckDB (local `warehouse.duckdb` or query over S3).
- **dbt**: Staging models clean raw data; marts build standings (from game results) and player per-game stats (from season totals).

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

- **games** – one row per game (date, visitor, home, points, etc.); all months of the season are fetched and concatenated.
- **player_season_totals** – raw counting stats per player (G, MP, FG, FGA, PTS, …); one row per player per team if traded.
- **roster** – one row per player–team–season (all 30 teams).

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

- **Staging**: `stg_games`, `stg_player_totals`, `stg_roster` – light cleanup and column renames from `raw`.
- **Marts**:
  - **standings** – conference standings (W, L, W/L%, GB) derived from game results only.
  - **player_per_game** – per-game stats (PPG, RPG, APG, …) derived from raw season totals (e.g. PTS/G).

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
├── basketball_reference.py   # Scraper (raw tables only used by ingest)
├── ingest/
│   └── load_warehouse.py      # Scrape → DuckDB (+ optional S3)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml          # DuckDB path; use DBT_PROFILES_DIR=dbt
│   ├── packages.yml
│   └── models/
│       ├── sources.yml       # raw.games, raw.player_season_totals, raw.roster
│       ├── staging/
│       └── marts/
├── warehouse.duckdb          # Created by ingest (path configurable)
└── requirements.txt
```

## Notes

- Be polite when scraping: the ingest uses a short delay between requests.
- Raw data only: no standings or per-game tables are scraped; those are built in dbt from games and player totals.
