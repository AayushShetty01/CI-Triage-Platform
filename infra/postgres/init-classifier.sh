#!/bin/sh
set -eu

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=classifier_password="${CLASSIFIER_DB_PASSWORD:-classifier_dev}" <<'SQL'
CREATE ROLE classifier LOGIN PASSWORD :'classifier_password';
CREATE SCHEMA classifier AUTHORIZATION classifier;
SQL
