# Custom Superset image with the extras needed to query Iceberg-on-MinIO via DuckDB.
# duckdb-engine is the SQLAlchemy dialect Superset uses to talk to DuckDB.
FROM apache/superset:4.1.1

USER root
# duckdb>=1.5.0: the `iceberg` extension only became a core, multi-arch
# (incl. linux_arm64) extension around this version — 1.1.3's was
# community-only and amd64-only, which is why this image used to need
# `platform: linux/amd64` in docker-compose.yml (now removed). Pinned exact
# rather than a floor since duckdb-engine's compatible range with newer
# DuckDB couldn't be verified without network access; confirm both with
# `docker compose build` and loosen duckdb-engine's pin if it doesn't resolve.
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    duckdb==1.5.3 \
    duckdb-engine>=0.13.4 \
    pyiceberg==0.8.1

# Pre-install DuckDB extensions used by SQL Lab so the first query is fast
# and doesn't need manual INSTALL commands.
USER superset
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL iceberg'); c.close()"
