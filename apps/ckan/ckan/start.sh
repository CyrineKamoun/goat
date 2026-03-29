#!/bin/sh
set -eu

DB_HOST="${POSTGRES_HOST:-ckan-db}"
DB_USER="${POSTGRES_USER:-ckan}"

echo "Waiting for database..."
while ! pg_isready -h "$DB_HOST" -U "$DB_USER" >/dev/null 2>&1; do
  sleep 2
done

echo "Ensuring datastore role and grants..."
export PGPASSWORD="${POSTGRES_PASSWORD:-ckan}"
psql -h "$DB_HOST" -U "$DB_USER" -d postgres -v ON_ERROR_STOP=1 <<-'EOSQL'
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datastore_ro') THEN
        CREATE ROLE datastore_ro LOGIN PASSWORD 'datastore';
    END IF;
END
$$;

SELECT 'CREATE DATABASE datastore'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datastore')\gexec
EOSQL

psql -h "$DB_HOST" -U "$DB_USER" -d datastore -v ON_ERROR_STOP=1 <<-'EOSQL'
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

GRANT CONNECT ON DATABASE datastore TO datastore_ro;
GRANT USAGE ON SCHEMA public TO datastore_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO datastore_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datastore_ro;
EOSQL
unset PGPASSWORD

echo "Waiting for Solr..."
while ! wget -qO /dev/null http://solr:8983/solr/ckan/admin/ping; do
  sleep 2
done

echo "Initializing CKAN database..."
ckan -c /etc/ckan/production.ini db init || true

ADMIN_NAME="${CKAN_SYSADMIN_NAME:-admin}"
ADMIN_PASSWORD="${CKAN_SYSADMIN_PASSWORD:-admin123}"
ADMIN_EMAIL="${CKAN_SYSADMIN_EMAIL:-admin@localhost}"

echo "Creating sysadmin user..."
if ! ckan -c /etc/ckan/production.ini user list | grep -q "^name=${ADMIN_NAME}$"; then
  ckan -c /etc/ckan/production.ini user add "$ADMIN_NAME" email="$ADMIN_EMAIL" password="$ADMIN_PASSWORD" || true
fi
ckan -c /etc/ckan/production.ini sysadmin add "$ADMIN_NAME" || true

echo "Initializing harvest tables..."
ckan -c /etc/ckan/production.ini db upgrade -p harvest || true

echo "Initializing spatial tables..."
ckan -c /etc/ckan/production.ini db upgrade -p spatial || true

echo "Bootstrapping harvest sources from config..."
python3 /usr/local/bin/bootstrap_harvest_sources.py || true

echo "Rebuilding Solr search index..."
ckan -c /etc/ckan/production.ini search-index rebuild || true

echo "Starting CKAN..."
exec ckan -c /etc/ckan/production.ini run --host 0.0.0.0 --port 5000
