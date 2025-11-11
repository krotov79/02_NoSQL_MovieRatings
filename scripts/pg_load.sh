#!/usr/bin/env bash
set -euo pipefail
DB="moviedb"
USER="postgres"
HOST="localhost"
PORT="5432"
export PGPASSWORD=postgres

psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -f scripts/pg_schema.sql

psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c "\copy movies  FROM 'data/movies.csv'  CSV HEADER"
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c "\copy users   FROM 'data/users.csv'   CSV HEADER"
psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -c "\copy ratings FROM 'data/ratings.csv' CSV HEADER"

echo "Postgres loaded."
