# Learning notes

A walkthrough of the concepts in this repo and why each piece is the way it is.
Read this if you're new to data engineering.

## The shape of a modern data pipeline

```
[ source API ]  ─▶  [ raw landing (bronze) ]  ─▶  [ cleaned (silver) ]  ─▶  [ aggregates (gold) ]  ─▶  [ consumers ]
                         immutable                  typed, deduped         business-ready          dashboards, APIs
```

This is the **medallion architecture**. Three rules:

1. **Bronze is immutable.** Whatever the upstream sent, store byte-for-byte.
   Never mutate. If silver/gold logic changes, re-derive them from bronze.
2. **Silver is the typed source of truth for the business.** Schema-enforced,
   deduped, joined where natural. Almost every analytical query starts here.
3. **Gold is shaped to a use case.** Daily aggregates per zone, "next-24h"
   serving views, model features. One gold table per consumer pattern.

The big payoff: silver/gold logic is *replaceable*. Schema bug? Vendor change?
New format? Re-derive from bronze. You never lose history.

## Glossary

### Lakehouse
A data lake (object storage + open file formats) augmented with a **table
format** (Iceberg/Delta/Hudi) so you get database-like guarantees — schema,
ACID, time travel — without a database. Cheap object storage, any query
engine, no vendor lock-in.

### Object storage
S3 or anything S3-compatible (MinIO, Hetzner Object Storage, Cloudflare R2,
Backblaze B2). Files in buckets, accessed by HTTP. Cheap, durable, infinite.
Slow per-request relative to a local disk; columnar formats are designed
around this.

### Parquet
On-disk **columnar** file format. Compressed per-column (so same-typed values
compress together — typically 5–10× smaller than CSV/JSON). Has per-column
statistics in each file's footer (min, max, null count). Lets engines skip
reading row groups that can't possibly match a filter — "predicate pushdown".

### Apache Arrow
**In-memory columnar** standard. Parquet is the on-disk format; Arrow is the
in-RAM representation. Decouples storage from compute and lets every modern
engine (DuckDB, Polars, pandas 2.x, Spark, etc.) hand data to one another with
zero serialisation cost.

### Iceberg (table format)
A pile of small JSON+Avro files that turn "a folder of Parquet files" into "a
table". Tracks:

- **Snapshots** — every commit is a new snapshot. Atomic pointer swap = ACID.
  Time travel is "read an older snapshot's manifest".
- **Schema evolution** — add/drop/rename columns without rewriting data; old
  files keep working because schemas reference field IDs (1, 2, 3…) not names.
- **Partition spec evolution** — change partitioning later; old data stays
  where it is, new data writes to the new layout.
- **File-level statistics** — manifest files record min/max/null per column
  per data file. Engines prune files without opening them.

### Catalog (Nessie, Polaris, Glue, Hive Metastore, …)
The "address book" for tables. Stores one tiny pointer per table —
`silver.prices_hourly → s3://.../v42.metadata.json` — and atomically swaps
that pointer on commit (compare-and-swap). It does **not** see your data.

**Nessie**'s killer feature is **git-like branches and tags over the catalog
state**. Want to test a schema change? Branch the catalog, run the change on
the branch, validate, merge. Without branches you'd be making `_v2` tables and
manual swaps.

### Dagster (orchestrator)
Schedules and runs the data work. The interesting design choice: it's
**asset-oriented**. You declare datasets and how they're produced; Dagster
derives the dependency graph (= lineage) automatically. Compare to Airflow,
where you declare tasks and bolt lineage on later.

Key concepts here:

- **Asset** — a Python function whose output is a piece of data.
- **Partition** — a slice of an asset along a dimension (here: by day).
- **Materialise** — actually run the function for a given partition.
- **Resource** — an external system (HTTP client, catalog) injected into
  assets. Lets you swap implementations for tests/dev/prod.
- **Schedule / Sensor** — what triggers a materialisation: cron, file arrived,
  upstream updated, etc.

### DuckDB (query engine)
Embedded analytical SQL engine — like SQLite but column-oriented. Runs
in-process inside whatever Python program loads it. Reads Parquet/Iceberg/CSV
directly from S3 via the `httpfs` and `iceberg` extensions. Best price for
single-machine analytics today.

When does it stop being enough? You'd notice if a single query needed to scan
> 100 GB or wanted distributed parallelism. Then swap to **Trino** (federated
SQL, reads the same Iceberg tables) without changing storage. That's the
lakehouse pattern's real payoff.

### Superset (BI)
Dashboard tool. Connects to anything SQLAlchemy-compatible, including DuckDB.
We use DuckDB-engine to scan Iceberg tables in MinIO from Superset directly.

### dbt (deferred to Phase 3)
SQL-based transformations with tests, docs, and column-level lineage. Replaces
the gold-layer Python with declarative SQL models.

## Why the choices in this repo

### Why columnar (Parquet) for time series?
Even though "time series row" feels row-shaped, every realistic query is
columnar: filter by time range, project a few columns, aggregate. Modern
time-series databases (InfluxDB v3, QuestDB, ClickHouse, TimescaleDB on old
chunks) are all columnar. Hard agreement in the industry.

### Why Iceberg over plain Parquet?
Schema enforcement, ACID writes, time travel, hidden partitioning, schema
evolution. Without it, every concurrent write or schema change is a footgun.

### Why the Apache Iceberg REST reference catalog (originally planned: Nessie)?
We first tried Project Nessie 0.99 because of its git-like branching feature,
but Nessie's required secret-URN indirection couldn't be configured via env
vars or a mounted properties file in a stable way (the `access-key` property
is declared as a URN-only String, with no inline credentials form, and the
default secret manager doesn't expose a Quarkus-config namespace we could
write to). After several iterations we swapped to the Apache Iceberg REST
reference catalog (`tabulario/iceberg-rest`) — same REST protocol from
PyIceberg's perspective, plain AWS-style env-var credentials, no URN games.

Trade-off: we lose Nessie's catalog branching. Two reasonable ways to get it
back later: (a) revisit Nessie when its docs catch up with current secret-
manager defaults, or (b) swap to Apache Polaris which uses real RBAC and
documented credential vending, plus is gaining the most momentum in 2026.

### Why Dagster over Airflow?
Asset-oriented model — you declare *what data exists* instead of *what jobs
run*. Lineage and observability are free. Local dev is one command.
Concepts transfer to Airflow if you ever switch.

### Why uv?
Lockfile-based reproducibility at 10–100× the speed of pip/poetry. The 2025-26
default for new Python projects.

### Why day partitioning on Iceberg?
Our write pattern is "overwrite by `delivery_date`". Day partitioning means
each daily run rewrites exactly one tiny file per zone — no write amplification.
The trade is many small files; we'll add periodic compaction in Phase 2.

### Why a serving layer for Home Assistant (Phase 5)?
The lake is great for analytics but slow-cold and coupled to schema choices.
A small Postgres "serving table" published from gold gives Home Assistant a
fast, stable interface and decouples the consumer from the lake's internals.
Standard "data product" pattern.

## What's deliberately not in Phase 1

- **dbt** — Phase 3. Replaces the gold-layer Python with SQL models, plus
  tests + column-level lineage for free.
- **OpenLineage / DataHub** — Phase 4. Wire-format lineage events out of
  Dagster + dbt for an external catalog UI.
- **FastAPI + serving Postgres** — Phase 5. The Home Assistant interface.
- **Compaction job** — Phase 2. Rewrite small daily files into bigger ones
  periodically.
- **More datasets** — energy mix, generation by source, cross-border flows.
  All available via ENTSO-E once the bronze pattern is comfortable.
- **Schema tests on silver/gold** — bare-minimum dbt tests will cover this in
  Phase 3.

## Known limitations / deferred TODOs

Things we explicitly accepted as good-enough for Phase 1 and will revisit:

- **Multi-dimensional partitions (date × zone)**. Bronze currently fans out
  internally across zones with a `try/except` per zone. The more idiomatic
  Dagster shape is `MultiPartitionsDefinition` on `(delivery_date,
  bidding_zone)`, giving native per-zone retry, backfill, and failure
  observability — at the cost of ~14k partitions/year per asset in the UI.
- **AssetCheck gates**. Add a Dagster asset check that fails materialization
  if fewer than N zones succeeded, or if a required zone (PL, DE_LU) is
  missing. Currently logged as warnings only.
- **Tie-handling for cheapest/peak hour**. `gold_prices_daily_stats` picks the
  earliest hour on ties via `ORDER BY price ASC, ts_utc ASC` — silently
  arbitrary. The `gold_cheapest_windows` asset addresses the common
  smart-home use case ("cheapest 2h block") but doesn't expose all ties.
- **Image pinning**. Postgres, Nessie, and the official Superset image should
  be pinned by digest (not just tag) for true reproducibility across rebuilds.
  MinIO and mc are already pinned by dated release tag.
- **Type-coercion friction in gold**. The explicit Arrow `.cast()` calls in
  gold assets exist because DuckDB returns `int64` for `COUNT(*)` while our
  Iceberg schema declares `int32`. dbt-iceberg handles this in Phase 3.
- **Sub-hourly resolution support**. `gold_cheapest_windows` filters to
  `resolution_minutes = 60`. DE-LU has been quarter-hourly since 2025-10;
  to include it we'd need to either pre-aggregate to hourly or compute
  windows in interval units rather than hours.
- **Iceberg compaction**. Day-level partitioning produces many small Parquet
  files. Phase 2 adds a periodic `rewrite_data_files` job that consolidates
  small files into larger ones, improving long-range query performance.

## Reading the code in order

If you're tracing the data path:

1. `src/energy_pipeline/entsoe/zones.py` — the catalogue of bidding zones.
2. `src/energy_pipeline/entsoe/client.py` — fetches one (zone, day) of XML.
3. `src/energy_pipeline/entsoe/parser.py` — XML → typed `PricePoint`.
4. `src/energy_pipeline/assets/bronze.py` — fan-out across zones, write raw XML
   to MinIO. Daily-partitioned Dagster asset.
5. `src/energy_pipeline/assets/silver.py` — read partition's bronze, parse,
   upsert into Iceberg.
6. `src/energy_pipeline/assets/gold.py` — DuckDB-aggregate silver to daily
   stats, upsert into Iceberg.
7. `src/energy_pipeline/resources.py` — how Dagster injects the ENTSO-E
   client and the Iceberg catalog.
8. `src/energy_pipeline/definitions.py` — wires everything together with a
   schedule.

## Operational mental model

- **One Dagster partition = one delivery day.**
- **Idempotent.** Re-running a partition produces the same final state
  (overwrite by `delivery_date` in silver/gold).
- **Bronze never changes.** If silver schema needs a new column, add it,
  backfill silver from existing bronze; bronze is untouched.
- **Failures are recoverable.** Dagster tracks each run; failed partitions can
  be re-materialised individually.

## Phasing roadmap

| Phase | Goal | What lands |
|---|---|---|
| 1 (this) | End-to-end ingest + lakehouse + dashboards | bronze/silver/gold, Iceberg+Nessie, Superset |
| 2 | Operational hardening | Iceberg compaction, schema tests, monitoring/alerting |
| 3 | dbt | Replace gold-layer Python with dbt models; column-level lineage |
| 4 | Lineage emission | OpenLineage events from Dagster+dbt; optional DataHub |
| 5 | Smart-home interface | Postgres serving layer, FastAPI, auth, Home Assistant integration |
| 6 | Forecasting | Price forecast model; load-shifting recommendations |
