"""Helpers shared between silver + gold assets."""

import s3fs

from energy_pipeline.config import settings


def s3_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=settings.minio_root_user,
        secret=settings.minio_root_password,
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

    Modern REST-catalog Iceberg readers (Trino REST, Spark REST, PyIceberg)
    don't need any of this — the catalog service tells them the current
    metadata pointer. This is purely a bridge so tools that don't speak the
    REST catalog protocol can still scan the table via its S3 path.

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
