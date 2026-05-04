# Custom Superset image with the extras needed to query Iceberg-on-MinIO via DuckDB.
# duckdb-engine is the SQLAlchemy dialect Superset uses to talk to DuckDB.
FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir \
    duckdb==1.1.3 \
    duckdb-engine==0.13.4 \
    pyiceberg==0.8.1
USER superset
