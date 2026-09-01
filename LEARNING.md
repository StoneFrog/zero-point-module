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

### dbt (Phase 3)
SQL-based transformations with tests, docs, and column-level lineage.
Replaces the gold-layer Python with declarative SQL models
(`dbt_project/models/gold/`). Orchestrated from Dagster via `@dbt_assets`
(`assets/gold_dbt.py`) — dbt's own tests (`_gold.yml`, plus the singular
test in `dbt_project/tests/`) surface as Dagster asset checks automatically.
Doesn't write Iceberg directly here — see "Why dbt writes to a scratch
DuckDB file instead of Iceberg directly" below.

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

### Why co-located asset checks instead of separate `@asset_check` functions? (Phase 2)
Dagster supports two shapes for a check: a standalone `@asset_check(asset=X)`
function, or a check declared via `check_specs=` on `@asset` itself, with the
asset function `yield`ing both its `Output` and its `AssetCheckResult`s. We
used the co-located form throughout (`bronze.py`, `silver.py`, `gold_dbt.py`,
`maintenance.py`). Reasons: it reuses values the asset already computed (no
re-scanning bronze/silver to check what the asset just wrote), it guarantees
the check runs against the exact partition the asset just materialised
rather than depending on partition-context being threaded through a
separately-scheduled check run, and — the one that actually matters for
correctness — because the check code runs *inline, before later statements
in the same function*, it can gate what those later statements do. A
standalone `@asset_check` can't do this even in principle: it only ever runs
*after* the asset it checks has already been materialised.

Worth being precise about what `yield`ing a check does and doesn't do.
`AssetCheckResult(blocking=True)` is a plain object — building it has no
side effects, and yielding it doesn't pause your own function; the engine
just consumes it as one item in the generator's event stream and *later*
uses it to decide whether to skip a downstream *op* in the run's execution
plan. That "skip the next op" behavior is identical whether the check came
from `check_specs=` or a standalone `@asset_check` — blocking isn't a
co-located-only feature. What's unique to co-located checks is purely a
matter of where you put the `yield` relative to your own side-effecting
code, and whether you act on the result yourself.

`silver_prices_hourly` actually uses that: `row_integrity` computes
`check_silver_row_integrity` and yields the result *before* the
`table.overwrite()` call, then explicitly checks `if not passed: return`
before reaching it — so a duplicate key or null price never reaches
Iceberg, not just "gets flagged after the fact." (Earlier versions of this
asset yielded the check but didn't branch on it, which meant bad rows got
written anyway and only *downstream* steps were skipped for that one run —
a real gap, since Dagster still shows the materialization as successful and
a later independent re-run of gold reads the bad data with no gate at all.)
`bronze.py`'s `zone_completeness` and `gold_dbt.py`'s `stats_consistency`
don't do this — they're WARN, not ERROR, and deliberately don't gate their
own writes:

- `zone_completeness` is WARN because a single zone outage shouldn't stop
  the run — matches the existing "log a warning and continue" pattern for
  per-zone fetch failures.
- `stats_consistency` is WARN because it's a belt-and-suspenders sanity
  check on dbt's output (dbt's own singular test already enforces the same
  invariant during `dbt build`); nothing downstream depends on
  `gold_prices_daily_stats` specifically — `gold_cheapest_windows` is a
  separate dbt model, independently computed from the same silver Parquet
  handoff, not from this table.

### Why file-count monitoring instead of automatic Iceberg compaction? (Phase 2)
LEARNING.md's Phase 1 note on day partitioning flagged "many small files"
as a known tradeoff, with compaction planned for Phase 2. We looked at
implementing it and backed off to monitoring-only, for a specific reason:
PyIceberg 0.8.1 (pinned in `pyproject.toml`) has no native
`rewrite_data_files`/compaction action — the feature has been an open
request upstream (`apache/iceberg-python#1092`) and still isn't part of the
0.9.0 release. Two ways to actually shrink file count exist in principle:

1. **Native compaction action** — not available in our pinned version.
2. **Hand-rolled**: evolve the partition spec to something coarser (e.g.
   month instead of day) and rewrite historical data under the new spec.
   We didn't ship this: our daily upsert pattern
   (`overwrite(df, overwrite_filter=EqualTo("delivery_date", day))`) relies
   on the filter aligning exactly with a file's partition boundary — day
   partitioning is what makes that safe. Coarsening the partition breaks
   that alignment for the *existing* daily-overwrite assets unless every
   write is also restructured to buffer a full month, a much bigger change
   than "operational hardening" scope, and one we have no live catalog here
   to test against.

So Phase 2 ships `assets/maintenance.py`'s `lake_file_health` asset instead:
a weekly, direct S3 listing of each managed table's `data/` prefix, reporting
file count and average file size as Dagster asset metadata, with a WARN
check when a table's average file size drops under ~1MB. It doesn't fix
anything — it gives a human the numbers to decide when a manual rebuild (or
a pyiceberg version bump, once the native action ships) is worth doing.

### Why a generic webhook for alerting instead of a Slack integration? (Phase 2)
`sensors.py`'s `pipeline_failure_sensor` always logs failures (visible in the
Dagster UI and daemon logs with no configuration). It optionally POSTs a
`{"text": "..."}` JSON payload to `ALERT_WEBHOOK_URL` if set — that shape
matches Slack's incoming-webhook format directly, but doesn't require a
Slack app, OAuth scopes, or a dedicated `dagster-slack` dependency for a repo
that may not have Slack at all. Swap in a real integration later by pointing
the same env var at whatever ingests generic JSON webhooks.

### Why dbt writes to a scratch DuckDB file instead of Iceberg directly (Phase 3)?
`dbt_project/models/gold/` computes both gold tables, but only as tables in a
local scratch `.duckdb` file (`profiles.yml`'s `dev` target). Two plain
Dagster assets (`gold_prices_daily_stats`, `gold_cheapest_windows` in
`assets/gold_dbt.py`) read that file back out and push it into Iceberg using
the exact same PyIceberg `overwrite()` pattern silver.py and the old Python
gold assets used, instead of having dbt write Iceberg itself.

Why the extra hop: we built this without network or Docker access, so there
was no way to verify that a dbt-duckdb Iceberg-write path would actually
preserve the exact schema, partition spec, and identifier fields already
committed to in `gold.py`/`gold_windows.py` (see those modules — the field
IDs and day+zone partitioning are load-bearing, and getting them wrong on a
rewrite means silently different tables, not a loud error). That write path
has been hardened over many iterations already (check the git log for the
trail of PyIceberg/Arrow type fixes it took to get right) and wasn't worth
re-risking on an unverified plugin. dbt's job here is the SQL, tests, and
docs — genuinely replacing the gold-layer Python per the Phase 3 goal — not
also being the thing that talks to the lake. Revisit once this has actually
been run against the live stack.

### Why the staging model reads silver via `iceberg_scan()` (and why it didn't at first)
`stg_silver_prices_hourly.sql` reads silver straight from its Iceberg S3
location with DuckDB's `iceberg_scan()`, filtered to the partition's
`delivery_date` — same idea as the README's "Inspecting the lake from the
CLI" example.

That wasn't the original design. `iceberg` was a *community* DuckDB
extension with no `linux/arm64` build for a long time — exactly why the
Superset service in docker-compose.yml used to pin `platform: linux/amd64`
— and the Dagster containers weren't platform-pinned, so loading it inside
dbt-duckdb would have silently broken the whole pipeline on Apple Silicon,
not just BI. The first version of this staging model routed around that
entirely: `assets/gold_dbt.py` scanned silver via PyIceberg in Python and
wrote it to a local Parquet file, and the staging model read that file with
`read_parquet()` instead — no DuckDB extensions, no platform risk, at the
cost of an extra hop.

`iceberg` was later promoted to a *core*, multi-arch DuckDB extension
(including `linux_arm64`) — but only from roughly DuckDB 1.5.0 on; we were
pinned to `duckdb>=1.1.3`, well before that. Once the pin was bumped (see
pyproject.toml's comment) and both Dockerfiles pre-install `httpfs`+
`iceberg` at build time (mirroring the pattern Superset's Dockerfile
already used, so no outbound network access is needed at container start —
see docker/dagster/Dockerfile), the platform risk that justified the
Parquet hop no longer applies, so it was removed along with Superset's
`platform: linux/amd64` pin. `assets/gold_dbt.py`'s `gold_dbt_assets` no
longer touches PyIceberg at all on the read side — dbt reads Iceberg
directly now. The *write* side is unaffected by any of this — see the note
above on why dbt still doesn't write Iceberg; that's a different, unrelated
limitation (DuckDB-Iceberg's write support can't do partitioned-table
UPDATE/DELETE or copy-on-write, which our overwrite-by-partition pattern
needs) that no version bump fixes.

### Why silver_prices_hourly is declared as a dbt source (Phase 3)
`stg_silver_prices_hourly.sql` reads silver via `iceberg_scan()`, not
`{{ ref() }}` or `{{ source() }}` — dbt has no native way to express "call
this table-valued function with this specific S3 path" through either of
those. The risk: without a `ref()`/`source()` call somewhere, dbt's manifest
records *zero* dependency between this model and anything upstream, which
means dagster-dbt has nothing to wire into the Dagster asset graph either —
`gold_dbt_assets` would show no dependency on the `silver_prices_hourly`
Dagster asset at all. That's not just a cosmetic lineage gap: with no
recorded dependency, Dagster's execution planner has no reason to run
`silver_prices_hourly` before `gold_dbt_assets` in the same job — they'd be
independent, parallel-eligible nodes, and gold could compute over
yesterday's committed silver snapshot instead of the one just written in
that run (Iceberg's atomic snapshots mean this can't corrupt anything, but
it would be silently wrong).

The fix: `dbt_project/models/staging/_sources.yml` declares
`silver_prices_hourly` as a dbt *source* with `meta.dagster.asset_key` set
to the real Dagster asset key — this is dbt/dagster-dbt's documented
mechanism for "a table produced by something outside dbt." The staging
model then has a line of the form `-- {{ source('lake',
'silver_prices_hourly') }}`: a SQL comment, not a Jinja comment, so dbt's
Jinja renderer still evaluates it (and therefore still records the
dependency) even though the actual rendered text is discarded — the real
`FROM` clause stays `iceberg_scan(...)`. `assets/gold_dbt.py`'s custom
`DagsterDbtTranslator` has to cooperate: it maps ordinary dbt models to a
bare (unprefixed) asset key, but explicitly defers to the base translator
for source nodes, since overriding `get_asset_key` for sources too would
bypass `meta.dagster.asset_key` and break this exact mapping.

### Why credentials never travel in URLs
`entsoe_api_token`, `minio_root_password` and `alert_webhook_url` are all
`SecretStr` in `config.py`, and the ENTSO-E token is sent in the
`SECURITY_TOKEN` header rather than the `securityToken` query parameter the
API also accepts.

The reason is what happens on failure, not on success. httpx puts the full
request URL into its exception messages and into its INFO-level
`HTTP Request: GET <url>` log line. `bronze_entsoe_day_ahead` logs `str(exc)`
for every zone whose fetch fails, and Dagster persists those log lines to
Postgres — so a credential carried in the URL becomes a credential at rest in
the event log the first time upstream returns an error, without any code doing
anything obviously wrong. The same reasoning covers `sensors.py`: for a
Slack-style incoming webhook the URL *is* the credential, so the delivery
error is scrubbed before it reaches the log.

Three layers, outermost first:

- Keep the secret out of the URL (header auth), so nothing stringifiable
  holds it in the first place.
- `SecretStr`, so a stray `print(settings)` or a logged model dump renders
  `**********` instead of the value.
- `redaction.redact()` as a backstop at the two places where an exception
  message reaches a log.

`tests/test_entsoe_client.py` pins the outermost layer by asserting the token
is absent from the request URL, so a revert to query-parameter auth fails the
suite rather than silently reintroducing the leak. If ENTSO-E ever rejects the
header on a GET, restoring `"securityToken": self._api_token` in `params` is
the fallback — the other two layers still hold, and that test is the one to
update.

### Why a serving layer for Home Assistant (Phase 5)?
The lake is great for analytics but slow-cold and coupled to schema choices.
A small Postgres "serving table" published from gold gives Home Assistant a
fast, stable interface and decouples the consumer from the lake's internals.
Standard "data product" pattern.

## What's deliberately not in Phase 1

- ~~**dbt**~~ — done in Phase 3. `dbt_project/models/gold/` replaces the
  gold-layer Python with SQL models + tests + docs; see "Why dbt writes to a
  scratch DuckDB file instead of Iceberg directly" above for the one place
  it's not a 1:1 swap.
- **OpenLineage / DataHub** — Phase 4. Wire-format lineage events out of
  Dagster + dbt for an external catalog UI.
- **FastAPI + serving Postgres** — Phase 5. The Home Assistant interface.
- **Compaction job** — Phase 2. Rewrite small daily files into bigger ones
  periodically.
- **More datasets** — energy mix, generation by source, cross-border flows.
  All available via ENTSO-E once the bronze pattern is comfortable.
- ~~**Schema tests on silver/gold**~~ — dbt tests on the gold models are done
  (`dbt_project/models/gold/_gold.yml` + the singular test in
  `dbt_project/tests/`); silver's equivalent invariants are still the
  hand-written Phase 2 asset check (`quality.check_silver_row_integrity`),
  not a dbt test, since silver is written by plain Python, not dbt.

## Known limitations / deferred TODOs

Things we explicitly accepted as good-enough for Phase 1 and will revisit:

- **Multi-dimensional partitions (date × zone)**. Bronze currently fans out
  internally across zones with a `try/except` per zone. The more idiomatic
  Dagster shape is `MultiPartitionsDefinition` on `(delivery_date,
  bidding_zone)`, giving native per-zone retry, backfill, and failure
  observability — at the cost of ~14k partitions/year per asset in the UI.
- ~~**AssetCheck gates**~~ — done in Phase 2. `bronze_entsoe_day_ahead` has a
  `zone_completeness` check (WARN if PL or DE_LU is missing);
  `silver_prices_hourly` has a blocking `row_integrity` check (duplicate
  keys, null prices, implausible interval counts); `gold_prices_daily_stats`
  has a `stats_consistency` sanity check. See `quality.py` and the "Why
  co-located asset checks" note above.
- **Tie-handling for cheapest/peak hour**. `gold_prices_daily_stats` picks the
  earliest hour on ties via `ORDER BY price ASC, ts_utc ASC` — silently
  arbitrary. The `gold_cheapest_windows` asset addresses the common
  smart-home use case ("cheapest 2h block") but doesn't expose all ties.
- **Image pinning**. Postgres, the Iceberg REST catalog, and the official
  Superset image should be pinned by digest (not just tag) for true
  reproducibility across rebuilds. MinIO and mc are already pinned by dated
  release tag.
- **Type-coercion friction in gold**. The explicit Arrow `.cast()` calls in
  the gold publish assets (`assets/gold_dbt.py`) exist because DuckDB
  returns `int64` for `COUNT(*)` while our Iceberg schema declares `int32`.
  Still here after Phase 3 — dbt materializes gold in DuckDB, not Iceberg
  (see "Why dbt writes to a scratch DuckDB file instead of Iceberg directly"
  above), so this cast just moved from the old Python gold assets to the new
  publish assets, not away. Would go away if dbt ever writes Iceberg
  directly here.
- **Sub-hourly resolution support**. `gold_cheapest_windows` filters to
  `resolution_minutes = 60`. DE-LU has been quarter-hourly since 2025-10;
  to include it we'd need to either pre-aggregate to hourly or compute
  windows in interval units rather than hours.
- **Iceberg compaction**. Day-level partitioning produces many small Parquet
  files. Phase 2 added *monitoring* (`lake_file_health`, weekly) but not
  automatic compaction — PyIceberg 0.8.1 has no native `rewrite_data_files`
  action to build on. See the "Why file-count monitoring instead of
  automatic Iceberg compaction" note above for the full reasoning and what
  would unblock this (a pyiceberg upgrade once the action ships, or a
  partition-spec-evolution rewrite of the daily-overwrite assets).

## Reading the code in order

If you're tracing the data path:

1. `src/energy_pipeline/entsoe/zones.py` — the catalogue of bidding zones.
2. `src/energy_pipeline/entsoe/client.py` — fetches one (zone, day) of XML.
3. `src/energy_pipeline/entsoe/parser.py` — XML → typed `PricePoint`.
4. `src/energy_pipeline/assets/bronze.py` — fan-out across zones, write raw XML
   to MinIO. Daily-partitioned Dagster asset.
5. `src/energy_pipeline/assets/silver.py` — read partition's bronze, parse,
   upsert into Iceberg.
6. `dbt_project/models/staging/stg_silver_prices_hourly.sql` and
   `dbt_project/models/gold/*.sql` — the gold-layer SQL (Phase 3), reading
   the partition's silver rows via `iceberg_scan()`.
7. `src/energy_pipeline/assets/gold_dbt.py` — orchestrates the dbt run
   (`gold_dbt_assets`) and publishes its output to Iceberg
   (`gold_prices_daily_stats`, `gold_cheapest_windows`); `assets/gold.py` and
   `assets/gold_windows.py` now hold just the Iceberg schema/table-lifecycle
   half of that.
8. `src/energy_pipeline/resources.py` — how Dagster injects the ENTSO-E
   client and the Iceberg catalog.
9. `src/energy_pipeline/quality.py` — pure data-quality checks (Phase 2),
   called from bronze/silver/gold_dbt via `check_specs=`.
10. `src/energy_pipeline/assets/maintenance.py` — small-file monitoring
    (Phase 2); `src/energy_pipeline/sensors.py` — run-failure alerting
    (Phase 2).
11. `src/energy_pipeline/dbt_resource.py` — where the dbt project lives, used
    by both `definitions.py`'s `DbtCliResource` and `assets/gold_dbt.py`.
12. `src/energy_pipeline/definitions.py` — wires everything together with
    schedules, a sensor, and the dbt resource.

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
| 1 | End-to-end ingest + lakehouse + dashboards | bronze/silver/gold, Iceberg REST catalog, Superset |
| 2 | Operational hardening | AssetCheck data-quality gates, small-file monitoring (compaction itself deferred — see notes above), run-failure alerting |
| 3 (this) | dbt | Gold layer replaced with dbt models (tests + docs); dbt orchestrated via `@dbt_assets`, dbt tests surface as Dagster checks |
| 4 | Lineage emission | OpenLineage events from Dagster+dbt; optional DataHub |
| 5 | Smart-home interface | Postgres serving layer, FastAPI, auth, Home Assistant integration |
| 6 | Forecasting | Price forecast model; load-shifting recommendations |
