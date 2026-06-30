# European Energy Price Pipeline

A learning-oriented data engineering project: pull day-ahead electricity prices
for every European bidding zone from ENTSO-E, land them in a local lakehouse
(MinIO + Iceberg + Nessie), transform via Dagster, query via DuckDB, visualise
via Superset. End goal: feed a smart-home system that shifts loads to cheap
hours.

## Status

**Phase 1 (this repo):** ingest → bronze → silver → gold → dashboards.
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
MinIO  s3://lake/warehouse/energy/prices_hourly/...   (typed, deduped)
     │  Dagster asset:  gold_prices_daily_stats       (Iceberg)
     ▼
MinIO  s3://lake/warehouse/energy/prices_daily_stats/ (aggregates)
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
| Dagster UI | <http://localhost:3000> | — |
| MinIO console | <http://localhost:9001> | `minioadmin` / `minioadmin` |
| Iceberg REST | <http://localhost:8181/v1/config> | — |
| Superset | <http://localhost:8088> | `admin` / `admin` |

## First run: materialise the pipeline

1. Open <http://localhost:3000>.
2. Go to **Assets**. You should see three assets under groups `bronze`,
   `silver`, `gold`.
3. Click **Materialize all** for any partition (e.g. `2026-05-04`). Dagster will
   run bronze → silver → gold in order.
4. Inspect each asset's metadata: row counts, snapshot IDs, S3 paths.

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
        FROM iceberg_scan('s3://lake/warehouse/energy/prices_hourly')
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
   SELECT * FROM iceberg_scan('s3://lake/warehouse/energy/prices_hourly')
   ORDER BY ts_utc DESC
   LIMIT 100;
   ```
7. Save the working query as a Dataset → build a Chart → drop on a Dashboard.

## Running tests outside Docker

```bash
uv sync
uv run pytest
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
├── src/energy_pipeline/
│   ├── config.py               # pydantic settings (one place for env vars)
│   ├── resources.py            # ENTSO-E + Iceberg catalog resources
│   ├── definitions.py          # Dagster Definitions (assets, jobs, schedule)
│   ├── entsoe/
│   │   ├── client.py           # HTTP client + retries
│   │   ├── parser.py           # XML -> typed PricePoint[]
│   │   └── zones.py            # bidding-zone EIC codes
│   └── assets/
│       ├── bronze.py           # raw XML to S3
│       ├── silver.py           # parsed -> Iceberg
│       └── gold.py             # daily aggregates -> Iceberg
├── tests/
│   ├── fixtures/               # offline ENTSO-E XML
│   └── test_parser.py
├── pyproject.toml + uv.lock
├── README.md
├── LEARNING.md                 # concepts, why-not-that, phasing
└── LICENSE                     # PolyForm Noncommercial 1.0.0
```

## License

[PolyForm Noncommercial License 1.0.0](LICENSE). Free for personal, hobby,
research, educational, and non-profit use; commercial use requires a separate
licence from the author.
