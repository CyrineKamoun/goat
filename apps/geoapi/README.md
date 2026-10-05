# GOAT GeoAPI

OGC API - Features and Tiles for GOAT layers, served from DuckLake with DuckDB.

## Overview

GeoAPI serves the **data** of a layer; core holds its metadata. It provides:

- **Features**: OGC API - Features with CQL2 filtering, sorting and paging (`/collections/{layerId}/items`).
- **Vector tiles**: OGC API - Tiles as MVT, from prebuilt PMTiles where they exist, generated on the fly otherwise.
- **Editing**: create, update and delete features and columns, and batch edits of a bundle's network layers.
- **Expressions**: validating and previewing computed-column expressions.
- **Download**: exporting a layer of a published project in common GIS formats.

The collection id is the layer UUID. The full, current route list is the OpenAPI document at `/api/docs`.

## Why a Separate Service?

Tiles and features are the latency-critical path every map view hits. Keeping them here, and long analytics in processes, means a heavy job never slows down map rendering.

## Storage Model

- Each layer is one DuckLake table, `t_<layer id without hyphens>`. New layers live in schema `main`; layers created before the flat layout stay in their old `user_<id>` schema and are found by an indexed lookup in DuckLake's own Postgres catalog.
- **Feature ids are `rowid + 1`**, because MapLibre treats an MVT feature id of 0 as "unset". The bundle editor keys on the layer's `id` column instead, since deletes within one batch would shift rowids.
- **Promoted catalog datasets** are not DuckLake tables: they are parquet files under `CATALOG_LAYERS_DIR`, exposed through an in-memory view that adds the same `rowid`. They are read-only.
- **PMTiles** live in `TILES_DATA_DIR` (user layers) and `CATALOG_TILES_DIR` (catalog layers). They are built outside geoapi, by goatlib tasks.
- **Workflow previews** are parquet and PMTiles files under `/app/data/temporary/user_<id>/…`, read with `?temp=true`.

## How It Stays Consistent

- **Reads are pinned to a DuckLake snapshot.** A background thread polls for new snapshots and swaps in a fresh, warmed connection off the request path. After a write, the pod also publishes on the Redis channel `ducklake:changed` so other pods refresh at once; the poll is the fallback when Redis is unavailable.
- **Every write deletes the layer's PMTiles**, so tiles fall back to dynamic generation until the scheduled Windmill task `rebuild_edited_pmtiles` rebuilds them once the layer has been idle for a while. Schema changes do not create a DuckLake snapshot, which is why the files are removed rather than trusted.
- **Bundle edits are optimistic.** The client sends the bundle revision it edited; a stale revision answers 409. The edges and nodes layers are written in one DuckLake transaction, then a `bundle_artifact_rebuild` job is sent to processes (`GOAT_PROCESSES_URL`).
- **Caches**: layer metadata and PMTiles-served tiles are cached in Redis; dynamic tiles and feature responses use ETags derived from the layer version and the pinned snapshot instead. Without Redis everything still works, only slower.

## Access Rules

- A layer of a published project is readable by anyone. Otherwise `customer.can('layer', …, 'read')` in core's database decides. **Read authorization runs in shadow mode by default** (`GEOAPI_ENFORCE_READ_AUTHZ=false`): a denial is only logged as `read_authz.would_deny`.
- Writes need a user and pass `customer.layer_write_allowed`. Features of a bundle member layer are edited through the bundle routes, though columns may still be added to it; catalog layers cannot be edited.
- Download needs no token but only works for layers of a published project.
- With `AUTH=False`, requests without a token run as the built-in development user.

## Gotchas

- **Settings use the `GEOAPI_` prefix.** A bare name works only for the settings that read it explicitly (for example `AUTH`, `KEYCLOAK_SERVER_URL`, `POSTGRES_*`, `DUCKLAKE_DATA_DIR`, `DUCKLAKE_PIN_SNAPSHOT`, `REDIS_URL`, `GOAT_PROCESSES_URL`); everything else, such as `ENFORCE_READ_AUTHZ`, `DUCKLAKE_POOL_SIZE` or the timeouts, must be set as `GEOAPI_<NAME>`. All settings are in `src/geoapi/config.py`.
- Point `DUCKLAKE_POSTGRES_SERVER` at Postgres directly, not through a transaction pooler: DuckLake attaches are long-lived and sit idle in a transaction.
- Memory per pod is roughly `GEOAPI_DUCKDB_MEMORY_LIMIT × GEOAPI_DUCKLAKE_POOL_SIZE`, because pooled cursors share one DuckDB instance. Set `GEOAPI_DUCKDB_THREADS` to the container's CPU limit.
- Updating a row inserted in the same DuckLake transaction leaves it with a transaction-local rowid that breaks MVT feature ids; `bundle_edit_service.py` writes derived columns in the insert itself for that reason.
- Never enumerate all DuckLake tables (`information_schema`, `SHOW TABLES`) on a request path: it loads every table's metadata, which takes very long on a large catalog.

## Development

Load the repo's root `.env` and run on port 8100, which is what the web app expects:

```bash
set -a && . ./.env && set +a          # from the repo root
cd apps/geoapi
uv run fastapi dev src/geoapi/main.py --port 8100

uv run pytest tests/                  # mocked, needs no services
uv run pytest tests/integration -m integration   # needs Postgres with the customer schema
```

At runtime it needs Postgres (core's `customer` schema and the DuckLake catalog), the shared data volume, and optionally Redis. The Docker image runs `fastapi run` on port 8000 with the DuckDB extensions and `tippecanoe-overzoom` baked in.
