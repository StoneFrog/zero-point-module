# Custom Superset image with the extras needed to query Iceberg-on-MinIO via DuckDB.
# duckdb-engine is the SQLAlchemy dialect Superset uses to talk to DuckDB.
FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    duckdb==1.1.3 \
    duckdb-engine==0.13.4 \
    pyiceberg==0.8.1

# Pre-install httpfs (a "known" DuckDB extension) so the first S3 query is fast.
# We intentionally do NOT pre-install iceberg — it's a community extension that
# doesn't publish linux_arm64 builds for DuckDB 1.1.3, which fails Apple Silicon
# hosts. Install it once at runtime via SQL Lab (see LEARNING.md).
USER superset
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs'); c.close()"
