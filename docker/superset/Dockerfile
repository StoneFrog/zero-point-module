# Custom Superset image with the extras needed to query Iceberg-on-MinIO via DuckDB.
# duckdb-engine is the SQLAlchemy dialect Superset uses to talk to DuckDB.
FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    duckdb==1.1.3 \
    duckdb-engine==0.13.4 \
    pyiceberg==0.8.1

# Pre-install the DuckDB extensions Superset will need at query time.
# iceberg is a community extension, so autoinstall_known_extensions doesn't
# cover it; httpfs is core but bundling both avoids first-query latency
# from the CDN fetch.
# Install as the superset user so the extensions cache lives in that user's
# home dir and is found at runtime.
USER superset
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL iceberg FROM community'); c.close()"
