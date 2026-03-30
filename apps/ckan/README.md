# Self-hosted CKAN (GOAT)

This folder provides a standalone CKAN stack with spatial + harvest extensions.

Harvesting is CKAN-owned in this setup:

- Harvest source definitions are declarative in `apps/ckan/ckan/harvest_sources.json`.
- CKAN bootstrap creates missing sources on startup.
- CKAN gather/fetch consumers and scheduler run as dedicated compose services.

## Quick start

1. Copy env file:

```bash
cp apps/ckan/.env.example apps/ckan/.env
```

2. Start CKAN stack:

```bash
cd apps/ckan
docker compose up -d --build
```

3. Verify CKAN is healthy:

```bash
curl http://127.0.0.1:5050/api/3/action/status_show
```

4. Check container status:

```bash
docker compose ps
```

5. Verify configured harvest sources:

```bash
docker compose exec -T ckan ckan -c /etc/ckan/production.ini harvester sources
```

6. Inspect metadata in PostgreSQL:

```bash
# list tables
docker compose exec -T db psql -U ckan -d ckan -c "\\dt"

# sample harvested datasets
docker compose exec -T db psql -U ckan -d ckan -c "select id, name, title, metadata_modified from package order by metadata_modified desc limit 20;"

# harvest state
docker compose exec -T db psql -U ckan -d ckan -c "select id, source_id, status, created from harvest_job order by created desc limit 20;"

# harvested objects by source
docker compose exec -T db psql -U ckan -d ckan -c "select harvest_source_id, count(*) as objects from harvest_object group by harvest_source_id order by count(*) desc;"

# readiness signal: most recent job status per source
docker compose exec -T db psql -U ckan -d ckan -c "select distinct on (source_id) source_id, status, created from harvest_job order by source_id, created desc;"
```

Host connection (DBeaver/pgAdmin/psql):

- Host: `127.0.0.1`
- Port: `5433`
- Database: `ckan`
- User: `ckan`
- Password: `ckan`

## Catalog Scripts Integration

Use `ckan_api` source type and the CKAN action API endpoint:

- URL format: `http://<host>:5050/api/3/action/package_search`
- For private CKAN, set `CKAN_API_KEY` in your runtime environment

Example datacatalog source URL when both services run in one Docker network:

- `http://ckan:5000/api/3/action/package_search`

## Notes

- The CKAN stack here is intentionally standalone.
- Adjust secrets and credentials before non-local deployments.
- If you run catalog scripts outside Docker, use `http://127.0.0.1:5050`.
- If you started this stack before datastore initialization changes, recreate DB volume:

```bash
docker compose down -v
docker compose up -d --build
```

## Catalog Pipeline Scripts

CKAN in this folder is runtime-only. The supported catalog ingestion scripts
now live in:

- `scripts/windmill/catalog/`

Run commands from the repository root using your existing environment:

```bash
python scripts/windmill/catalog/datacatalog_pipeline.py
python scripts/windmill/catalog/catalog_ai_flow.py
```

Sync scripts to Windmill with:

```bash
python scripts/windmill/sync-datacatalog-pipeline.py
```
