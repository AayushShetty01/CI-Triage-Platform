#!/bin/sh
set -eu
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=query_password="${QUERY_DB_PASSWORD:-query_dev}" <<'SQL'
CREATE ROLE query LOGIN PASSWORD :'query_password';
CREATE SCHEMA query AUTHORIZATION query;
SQL
