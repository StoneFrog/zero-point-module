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
    """Write metadata/version-hint.text so Hive-style scanners can find the
    current metadata without going through the REST catalog.

    Modern REST-catalog Iceberg tables (like ours) skip this file — the
    catalog service is the source of truth. But DuckDB's iceberg extension
    (and Trino's Hive catalog compat mode) still look for it when scanning
    a table by S3 path. Writing it is a cheap belt-and-braces so both
    catalog-aware and directory-mode readers work.

    Returns the version number that was written.
    """
    metadata_location = table.metadata_location  # e.g. s3://…/metadata/00042-<uuid>.metadata.json
    prefix, filename = metadata_location.rsplit("/", 1)
    metadata_dir_no_scheme = prefix.replace("s3://", "", 1)
    # PyIceberg names metadata files as "<zero-padded-version>-<uuid>.metadata.json".
    version = int(filename.split("-", 1)[0])
    fs = s3_fs()
    with fs.open(f"{metadata_dir_no_scheme}/version-hint.text", "wb") as f:
        f.write(str(version).encode("utf-8"))
    return version
