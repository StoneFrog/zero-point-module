# Custom Superset image with the extras needed to query Iceberg-on-MinIO via DuckDB.
# duckdb-engine is the SQLAlchemy dialect Superset uses to talk to DuckDB.
FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    duckdb==1.1.3 \
    duckdb-engine==0.13.4 \
    pyiceberg==0.8.1

# Pre-install DuckDB extensions used by SQL Lab so the first query is fast
# and doesn't need manual INSTALL commands. iceberg is available in the
# default extension repo for the amd64 platform this image runs on.
USER superset
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL iceberg'); c.close()"
