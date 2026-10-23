#!/bin/sh
set -eu

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=ingestion_password="$INGESTION_DB_PASSWORD" <<'SQL'
CREATE ROLE ingestion LOGIN PASSWORD :'ingestion_password';
CREATE SCHEMA ingestion AUTHORIZATION ingestion;
SQL
