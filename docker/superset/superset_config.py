"""Superset configuration. Mounted at /app/pythonpath/superset_config.py."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Persist Superset metadata (dashboards, charts, users) in our shared Postgres.
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
    f"@postgres:5432/superset"
)

# Allow uploading CSVs through the UI (handy for ad-hoc what-if analysis).
FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Trust the X-Forwarded-* headers if you ever put Superset behind a reverse proxy.
ENABLE_PROXY_FIX = True

# DuckDB connections need to install + load extensions on every connection.
# This is configured per-database in the Superset UI (see README).
