#!/bin/sh
set -eu

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=parser_password="${PARSER_DB_PASSWORD:-parser_dev}" <<'SQL'
CREATE ROLE parser LOGIN PASSWORD :'parser_password';
CREATE SCHEMA parser AUTHORIZATION parser;
SQL
