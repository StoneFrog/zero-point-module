"""Helpers shared between silver + gold assets."""

import s3fs

from energy_pipeline.config import settings


def s3_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=settings.minio_root_user,
        secret=settings.minio_root_password.get_secret_value(),
        client_kwargs={"endpoint_url": settings.s3_endpoint},
    )


def write_version_hint(table) -> int:
    """Make an Iceberg table readable by directory-mode / Hive-style scanners.

    Two files, both in the table's metadata/ prefix:

    - `version-hint.text`  — contains the current metadata version integer.
    - `v<version>.metadata.json` — a copy of the current metadata JSON at a
      filename DuckDB's iceberg extension recognises. PyIceberg names its
      canonical file "<zero-padded-version>-<uuid>.metadata.json", which
      neither DuckDB nor Hive-style tools can find via the version-hint.

    Anything that speaks the REST catalog needs none of this — the catalog
    tells it the current metadata pointer. Since Phase 3 that includes dbt:
    the staging model attaches the catalog and reads
    `lake.energy.prices_hourly` by name (see dbt_project/profiles.yml), so
    the pipeline itself no longer depends on these two files at all.

    They stay for Superset, which cannot take that route. Superset reaches
    DuckDB through duckdb_engine, whose connect options run `LOAD <ext>` and
    `SET <k>=<v>` but have no hook for issuing an `ATTACH`; and DuckDB does
    not persist attachments in the database file, so pointing Superset at a
    pre-attached file doesn't work either. Path-based `iceberg_scan()` is
    what's left, and it needs the version hint to find the current metadata.

    The cost of keeping them is a stale-read window: both files are written
    *after* the snapshot commits, so a path-based reader in between still
    sees the previous version. Harmless for daily batch publishes; worth
    remembering before pointing anything latency-sensitive at the S3 path.

    Returns the version number written.
    """
    metadata_location = table.metadata_location  # s3://…/metadata/00042-<uuid>.metadata.json
    prefix, filename = metadata_location.rsplit("/", 1)
    metadata_dir = prefix.replace("s3://", "", 1)
    version = int(filename.split("-", 1)[0])
    fs = s3_fs()

    # version-hint.text
    with fs.open(f"{metadata_dir}/version-hint.text", "wb") as f:
        f.write(str(version).encode("utf-8"))

    # v<version>.metadata.json — a same-content copy of the current metadata
    src_key = metadata_location.replace("s3://", "", 1)
    dst_key = f"{metadata_dir}/v{version}.metadata.json"
    with fs.open(src_key, "rb") as sf, fs.open(dst_key, "wb") as df:
        df.write(sf.read())

    return version


def count_data_files(bucket: str, table_prefix: str) -> tuple[int, int]:
    """Return (file_count, total_bytes) of Parquet data files under a table.

    A direct S3 listing under `<table>/data/`, not a PyIceberg metadata-table
    scan (`table.inspect.files()`) — this only needs a physical file count
    and doesn't care about per-file Iceberg stats, so it skips the catalog
    round-trip. See maintenance.py for what this feeds into.
    """
    fs = s3_fs()
    data_prefix = f"{bucket}/{table_prefix}/data"
    try:
        entries = fs.find(data_prefix, detail=True)
    except FileNotFoundError:
        return 0, 0
    file_count = 0
    total_bytes = 0
    for path, info in entries.items():
        if not path.endswith(".parquet"):
            continue
        file_count += 1
        total_bytes += info.get("size", 0)
    return file_count, total_bytes
