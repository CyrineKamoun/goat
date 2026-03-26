#!/bin/sh
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS postgis_topology;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE datastore'
    WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'datastore')\gexec
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "datastore" <<-EOSQL
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE EXTENSION IF NOT EXISTS postgis_topology;

        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datastore_ro') THEN
                CREATE ROLE datastore_ro LOGIN PASSWORD 'datastore';
            END IF;
        END
        $$;

        GRANT CONNECT ON DATABASE datastore TO datastore_ro;
        GRANT USAGE ON SCHEMA public TO datastore_ro;
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO datastore_ro;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO datastore_ro;
EOSQL
