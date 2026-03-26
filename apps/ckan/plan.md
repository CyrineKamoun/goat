# Datacatalog Architecture Plan

## Goal
Build a catalog architecture where a catalog server owns harvesting, scheduling, metadata, and versioning, while GOAT data processor owns full data download and processing for all catalog datasets. GOAT can help in versioning

## Core Decision
Use a dedicated catalog server as the system of record for metadata, and GOAT data processor as the data download and processing engine.

- Catalog server owns source definitions, harvesting, scheduling, metadata lifecycle, and provenance and verisoning if possible
- GOAT data processor owns downloads, enrichment, styling, quality checks, and materialization.
- Windmill is used as orchestrator for GOAT data processor workflows.
- GOAT production consumes catalog metadata and processed dataset availability through APIs.


## Target Responsibilities

### Catalog Server (System of Record)
- Manage source registry and source config.
- Run harvesting and schedule synchronization cycles.
- Store canonical metadata and source-side version history.
- Track legal/provenance metadata and policy outcomes.
- Expose API for listing datasets, source versions, and processing status references.

### GOAT data processor (Windmill + Workers)
- Execute heavy tasks: download, dedupe, AI enrichment, styling checks, and materialization.
- Process all catalog datasets according to schedule or pipeline rules (not end-user download requests).
- Produce processor-side versioned artifacts and quality/status outputs.
- Return execution results to catalog server; do not own catalog metadata API/state.

### GOAT Production
- Display catalog datasets and versions in UI.
- Read processed availability and versions from APIs.
- Let users attach/select already processed dataset versions.
- Create user-owned layer metadata and complete user import flow from preprocessed assets.

## Integration Contract (API First)
Define and freeze contract before implementation changes.

- `GET /v1/datasets`
- `GET /v1/datasets/{id}`
- `GET /v1/datasets/{id}/versions`
- `GET /v1/datasets/{id}/processor-versions`
- `GET /v1/processing-jobs/{job_id}`
- Optional webhook callback for processing completion

Required response fields:
- Dataset and version IDs
- Provenance (`source`, `fetched_at`, `checksum`, workflow/run references)
- License and attribution
- Policy check outcomes
- Artifact references for processed outputs

## Transitional Shared-DB Mode (Local/Test) TO IMPLEMENT FIRST
For local development and early test environments, run GOAT prod and processor against one PostgreSQL instance with strict schema boundaries.

### Schema Ownership
- Core-owned schemas (`customer` and existing core schemas): GOAT prod ownership only.
- `datacatalog` schema: catalog-service ownership only.
- Shared DuckLake and postgres base jsut different schema

### Access Rules
- GOAT prod role: no write grants on `datacatalog` tables.
- Catalog-service role: no write grants on Core-owned schemas.

### Data Path
1. Catalog server harvests sources and updates metadata/version records- saved in postgres of the harvesting instance if ckan or a schema if the harvester do not need a seperate strucutre like ckan
2. Windmill worker get notified when a harvest is done and launch GOAT data processor
3. GOAT data processor download process and style data.
4. GOAT prod reads datasets and processed versions via API and attaches selected version into user context.

## Windmill Topology
Preferred for production:
- Separate Windmill workspace and worker pool for catalog workflows.

Transitional option:
- Shared Windmill cluster with strict worker tags, queue isolation, concurrency limits, and separate credentials.


## Data Storage Locations

### Current local/test locations
If ckan 
- **CKAN metadata** is stored in PostgreSQL database `ckan` (notably tables `package`, `resource`, `harvest_*`).
-**Goat data Processor** stor date in same ducklake (schema datacataloge) as goat normal and store metadata with same strucutre as customer.layer  but in schema datacataloge
- No change in GOAT normal but data catalog should read data from schema data_catalog and if user wants a data in project thant create customer.layer a row (if not exist)

### Versioning model (as in the sketch)
- Keep **data versions** in DuckDB/DuckLake.
- Keep **metadata + style + processing state** in PostgreSQL.
- PostgreSQL records reference the active DuckDB version/table.

### Naming/versioning convention
- DuckDB table pattern: `ds_<dataset_id_hash>_v<version_num>`.

## Phased Delivery

### Phase 1 - Harvester
- Catalog server and metadata harvest setup.
- Work on how windmill know something was completely harvested

### Phase 2
- Implement a processor-side pipeline for dataset download and materialization into DuckLake (`catalog` schema).
- Add and maintain a PostgreSQL `datacatalog` schema for processor versions, artifact pointers, style references, and run status.
- Enforce traceable links between catalog metadata and processed dataset versions.

### Phase 3
- AI enrichment, styling checks, dedupe quality gates.

## Phase 4
- work on the UI data catalog to read form schema data-catalog and manage user upload data

## Verification Checklist
1. OpenAPI contract includes datasets, versions, jobs, provenance, and policy fields.
2. No code path writes across ownership boundaries directly in the database.
3. GOAT runs with catalog integration enabled or disabled via feature configuration.
4. End-to-end import succeeds from GOAT UI to user-owned layer creation.
5. Heavy catalog workflows do not degrade GOAT production latency.