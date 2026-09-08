#!/usr/bin/env bash
set -euo pipefail
# Runs only for a fresh PGDATA, under the official image's bootstrap superuser.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
REVOKE CONNECT, TEMPORARY ON DATABASE postgres FROM PUBLIC;
REVOKE CONNECT, TEMPORARY ON DATABASE template1 FROM PUBLIC;
SQL
for domain in product cart checkout order inventory payment shipping notification query; do
  password_var="${domain^^}_DB_PASSWORD"
  password="${!password_var:?Missing database password}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    --set=owner="${domain}_owner" --set=db="${domain}_db" --set=password="$password" <<'SQL'
CREATE ROLE :"owner" LOGIN PASSWORD :'password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
CREATE DATABASE :"db" OWNER :"owner";
REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE :"db" TO :"owner";
SQL
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${domain}_db" <<'SQL'
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO pg_database_owner;
SQL
done
touch "$PGDATA/platform-initialized"
