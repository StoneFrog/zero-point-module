# Build env for the Dagster webserver + daemon containers.
# Single image, two entrypoints (webserver / daemon) chosen via compose command.
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install uv (modern Python package manager — fast, deterministic lockfile).
COPY --from=ghcr.io/astral-sh/uv:0.8.17 /uv /usr/local/bin/uv

WORKDIR /opt/dagster/app

# Install dependencies first (cacheable layer). If uv.lock exists we use it;
# otherwise uv resolves and writes one at build time.
COPY pyproject.toml ./
COPY uv.loc[k] ./
RUN uv sync --no-install-project --no-dev

# Copy project source + README (hatchling reads README.md at build time because
# pyproject.toml declares `readme = "README.md"`).
COPY README.md ./
COPY src ./src
RUN uv sync --no-dev

# DAGSTER_HOME is where dagster.yaml + run state live.
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p ${DAGSTER_HOME}

# Run uv-managed binaries directly.
ENV PATH="/opt/dagster/app/.venv/bin:${PATH}"

# Pre-install DuckDB extensions (httpfs for S3, iceberg for the dbt staging
# model's iceberg_scan()) at build time, same reasoning as Superset's
# Dockerfile: first materialization doesn't need outbound network access
# for this, and it's cached into the image instead of re-downloaded on
# every container start.
RUN python -c "import duckdb; c=duckdb.connect(); c.execute('INSTALL httpfs'); c.execute('INSTALL iceberg'); c.close()"
