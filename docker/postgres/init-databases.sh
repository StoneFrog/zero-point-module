#!/usr/bin/env bash
# Create the three logical databases we need on first boot.
# Postgres official image runs every *.sh/*.sql here exactly once.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE dagster;
    CREATE DATABASE iceberg_catalog;
    CREATE DATABASE superset;
EOSQL
