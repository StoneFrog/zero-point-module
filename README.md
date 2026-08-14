# European Energy Price Pipeline

A learning-oriented data engineering project: pull day-ahead electricity prices
for every European bidding zone from ENTSO-E, land them in a local lakehouse
(MinIO + Iceberg), transform via Dagster + dbt, query via DuckDB, visualise
via Superset. End goal: feed a smart-home system that shifts loads to cheap
hours.

## Status

**Phase 1:** ingest → bronze → silver → gold → dashboards.
**Phase 2:** operational hardening — data-quality AssetChecks on
bronze/silver/gold, weekly small-file monitoring, run-failure alerting.
**Phase 3 (this repo):** gold layer replaced with dbt models (tests + docs),
orchestrated from Dagster via `@dbt_assets`.
See [LEARNING.md](LEARNING.md) for the conceptual walkthrough and the phasing
roadmap.

## Stack

```
ENTSO-E API
     │  Dagster asset:  bronze_entsoe_day_ahead       (daily partition)
     ▼
MinIO  s3://lake/bronze/entsoe/day_ahead/...raw.xml   (immutable raw)
     │  Dagster asset:  silver_prices_hourly          (Iceberg, day+zone partitioned)
     ▼
MinIO  s3://lake/silver/prices_hourly/...             (typed, deduped)
     │  dbt models (dbt_project/models/gold/), run via Dagster's gold_dbt_assets,
     │  published to Iceberg by gold_prices_daily_stats / gold_cheapest_windows
     ▼
MinIO  s3://lake/gold/prices_daily_stats/...          (aggregates)
MinIO  s3://lake/gold/prices_cheapest_windows/...     (smart-home load-shifting windows)
     │
     ▼
DuckDB (embedded query engine) ── Superset (BI)
```

| Concern | Tool |
|---|---|
| Source | ENTSO-E Transparency Platform |
| Orchestration | Dagster |
| Object storage | MinIO (S3-compatible) |
| File format | Parquet |
| Table format | Apache Iceberg |
| Catalog | Apache Iceberg REST reference catalog (`tabulario/iceberg-rest`) |
| Transformation (gold layer) | dbt (`dbt-duckdb`), orchestrated via `dagster-dbt` |
| Query engine | DuckDB |
| Visualisation | Apache Superset |
| Metastore (everyone) | PostgreSQL |
| Package manager | uv |

## Prerequisites

- Docker + Docker Compose v2
- (optional) `uv` if you want to run pipelines or tests outside Docker — `pip install uv` or see <https://docs.astral.sh/uv/>
- (optional, for live data) An ENTSO-E API token. Register at
  <https://transparency.entsoe.eu/usrm/user/createPublicUser>, then email
  `transparency@entsoe.eu` requesting API access. Until you have one, the
  pipeline runs against a bundled test fixture (24 hourly Polish prices for
  2026-05-04).

## Quick start

```bash
cp .env.example .env
# Edit .env if you have an ENTSO-E token; otherwise leave ENTSOE_USE_FIXTURE=true.
# Replace SUPERSET_SECRET_KEY with `openssl rand -base64 42`.

docker compose up -d --build
```

First boot takes a couple of minutes (image builds + Superset DB upgrade).
Watch progress with:

```bash
docker compose logs -f
```

When everything is healthy:

| Service | URL | Login |
|---|---|---|
| Dagster UI | <http://localhost:3001> | — (port via `DAGSTER_HOST_PORT`) |
| MinIO console | <http://localhost:9001> | `minioadmin` / `minioadmin` |
| Iceberg REST | <http://localhost:8181/v1/config> | — |
| Superset | <http://localhost:8088> | `admin` / `admin` |

## First run: materialise the pipeline

1. Open <http://localhost:3001> (or whatever you set `DAGSTER_HOST_PORT` to).
2. Go to **Assets**. You should see assets under groups `bronze`, `silver`,
   `gold` (the dbt models plus the `gold_prices_daily_stats` /
   `gold_cheapest_windows` publish assets), and an unpartitioned
   `lake_file_health` asset under `maintenance`.
3. Click **Materialize all** for any partition (e.g. `2026-05-04`). Dagster will
   run bronze → silver → dbt gold models → Iceberg publish, in order.
4. Inspect each asset's metadata: row counts, snapshot IDs, S3 paths, and the
   **Checks** tab for the data-quality results (`zone_completeness`,
   `row_integrity`, `stats_consistency`, plus dbt's own `not_null` tests and
   the singular test on `int_gold_prices_daily_stats`).

## Inspecting the lake from the CLI

Pop into the Dagster container and use DuckDB:

```bash
docker compose exec dagster-webserver bash
python - <<'PY'
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_endpoint='minio:9000';")
con.execute("SET s3_access_key_id='minioadmin';")
con.execute("SET s3_secret_access_key='minioadmin';")
con.execute("SET s3_use_ssl=false; SET s3_url_style='path';")
con.execute("INSTALL iceberg; LOAD iceberg;")
print(
    con.sql("""
        SELECT bidding_zone, ts_utc, price_eur_per_mwh
        FROM iceberg_scan('s3://lake/silver/prices_hourly')
        ORDER BY ts_utc, bidding_zone
        LIMIT 20
    """)
)
PY
```

## Connecting Superset to the lake

Superset reads Iceberg via DuckDB-engine. One-time setup:

1. Visit <http://localhost:8088>, log in (`admin` / `admin`).
2. **Settings → Database Connections → + Database**.
3. Pick **DuckDB**, then choose **Expose database** with this SQLAlchemy URI:
   ```
   duckdb:///:memory:
   ```
4. Open the new database's **Advanced → Other → Engine Parameters** and paste:
   ```json
   {
     "connect_args": {
       "config": {
         "s3_endpoint": "minio:9000",
         "s3_access_key_id": "minioadmin",
         "s3_secret_access_key": "minioadmin",
         "s3_use_ssl": false,
         "s3_url_style": "path"
       },
       "preload_extensions": ["httpfs", "iceberg"]
     }
   }
   ```
5. Test connection, save.
6. **SQL Lab** — try:
   ```sql
   SELECT * FROM iceberg_scan('s3://lake/gold/prices_daily_stats')
   ORDER BY delivery_date DESC
   LIMIT 100;
   ```
7. Save the working query as a Dataset → build a Chart → drop on a Dashboard.

## Running tests outside Docker

```bash
uv sync
uv run pytest
```

## Running dbt directly

Useful for iterating on the gold models without going through Dagster. Needs
a silver Parquet file to read (see `dbt_project/models/staging/`) — easiest
to grab one that `gold_dbt_assets` already wrote inside the running
container:

```bash
docker compose exec dagster-webserver bash
cd dbt_project
dbt build --vars '{"silver_parquet_path": "target/silver_partition.parquet", "window_hours": [1, 2, 3, 4, 6, 8]}'
dbt docs generate && dbt docs serve --port 8080  # column-level lineage in the browser
```

## Repository layout

```
.
├── docker-compose.yml          # full local stack
├── docker/                     # Dockerfiles + init scripts
│   ├── dagster/
│   ├── superset/
│   └── postgres/
├── dagster_home/               # Dagster instance config (mounted into containers)
│   ├── dagster.yaml
│   └── workspace.yaml
├── dbt_project/                 # dbt project: gold-layer SQL (Phase 3)
│   ├── dbt_project.yml
│   ├── profiles.yml             # committed — no secrets, all env_var()
│   ├── models/
│   │   ├── staging/stg_silver_prices_hourly.sql  # reads the Parquet handoff
│   │   └── gold/
│   │       ├── int_gold_prices_daily_stats.sql
│   │       ├── int_gold_cheapest_windows.sql
│   │       └── _gold.yml        # column docs + tests
│   └── tests/                   # singular tests (stats-consistency)
├── src/energy_pipeline/
│   ├── config.py                # pydantic settings (one place for env vars)
│   ├── resources.py             # ENTSO-E + Iceberg catalog resources
│   ├── dbt_resource.py          # dbt project location (Phase 3)
│   ├── quality.py               # pure data-quality checks (Phase 2)
│   ├── sensors.py               # run-failure alerting (Phase 2)
│   ├── definitions.py           # Dagster Definitions (assets, jobs, schedules, sensor, dbt resource)
│   ├── entsoe/
│   │   ├── client.py            # HTTP client + retries
│   │   ├── parser.py            # XML -> typed PricePoint[]
│   │   └── zones.py             # bidding-zone EIC codes
│   └── assets/
│       ├── bronze.py            # raw XML to S3 (+ zone_completeness check)
│       ├── silver.py            # parsed -> Iceberg (+ row_integrity check)
│       ├── gold.py              # prices_daily_stats: Iceberg schema/table lifecycle only
│       ├── gold_windows.py      # prices_cheapest_windows: Iceberg schema/table lifecycle only
│       ├── gold_dbt.py          # runs dbt, publishes its output to Iceberg (Phase 3)
│       └── maintenance.py       # weekly small-file monitoring (Phase 2)
├── tests/
│   ├── fixtures/                # offline ENTSO-E XML
│   ├── test_parser.py
│   ├── test_quality.py
│   └── test_sensors.py
├── pyproject.toml + uv.lock
├── README.md
├── LEARNING.md                  # concepts, why-not-that, phasing
└── LICENSE                      # PolyForm Noncommercial 1.0.0
```

## License

[PolyForm Noncommercial License 1.0.0](LICENSE). Free for personal, hobby,
research, educational, and non-profit use; commercial use requires a separate
licence from the author.
