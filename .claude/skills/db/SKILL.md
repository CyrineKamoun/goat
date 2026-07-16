---
name: db
description: Use when inspecting, debugging, or understanding the GOAT PostgreSQL database — querying projects, layers, users, orgs, teams, roles, scenarios, jobs, or checking data state during local dev.
---

# Database Query

Query the GOAT PostgreSQL database to inspect data, debug issues, and understand state.

## Connection

There is no host `psql`; go through the running Postgres container. The container name changes
across setups (`goat-db`, `goat-db18`, …), so discover it rather than hardcoding:

```bash
source /home/p4b/goat/.env
DBC=$(docker ps --format '{{.Names}}' | grep -E '^goat-db' | head -1)
docker exec -e PGPASSWORD=$POSTGRES_PASSWORD "$DBC" psql -h 127.0.0.1 -U $POSTGRES_USER -d $POSTGRES_DB
```

One-off query:

```bash
source /home/p4b/goat/.env
DBC=$(docker ps --format '{{.Names}}' | grep -E '^goat-db' | head -1)
docker exec -e PGPASSWORD=$POSTGRES_PASSWORD "$DBC" psql -h 127.0.0.1 -U $POSTGRES_USER -d $POSTGRES_DB -c "YOUR SQL HERE"
```

## Schemas

| Schema | Purpose |
|--------|---------|
| `customer` | Everything: users, orgs, teams, roles/permissions, projects, layers, scenarios, jobs, workflows |
| `ducklake` | DuckLake catalog (managed by geoapi, don't modify directly) |

## Key Tables & Relationships

The SQLModel definitions in `apps/core` are the source of truth — introspect when unsure:

```sql
SELECT table_name FROM information_schema.tables WHERE table_schema='customer' ORDER BY 1;
\d customer.layer
```

### Identity & sharing (all in `customer`)
- **user** (id uuid) — Keycloak-synced. firstname, lastname, avatar
- **organization** (id uuid) — name, avatar; **organization_domain**, **organization_analytics**
- **team** (id uuid) — belongs to org. name, avatar
- **role** (id uuid) — permission roles; RBAC via **permission**, **role_permission**, **user_role**, **resource**, **resource_grant**, **resource_permission**
- **user_team** — M2M user ↔ team; **invitation** — pending org/team invites
- **layer_organization / layer_team / layer_user** — layer sharing with role
- **project_organization / project_team / project_user** — project sharing with role

### Projects, layers & scenarios (`customer`)
- **project** (id uuid) — user_id, folder_id, active_scenario_id, layer_order[], basemap, tags[]
- **layer** (id uuid) — user_id, folder_id, data_store_id. Key fields: name, type, data_type, tool_type, feature_layer_type, feature_layer_geometry_type, extent (geometry), properties (jsonb), url, size, attribute_mapping (jsonb), in_catalog, tags[]
- **layer_project** (id int) — M2M layer ↔ project. name, properties (jsonb style config), other_properties, query (jsonb filters), charts, order, layer_project_group_id
- **layer_project_group** (id int) — layer groups. project_id, parent_id (self-ref nesting), order
- **data_store** (id uuid) — storage backends. type
- **folder** (id uuid) — user_id, name
- **job** (id uuid) — user_id. type, status, payload (jsonb)
- **scenario** (id uuid) — project_id, user_id, name
- **scenario_feature** (id uuid) — layer_project_id, feature_id (text), edit_type, geom, h3_3, h3_6. Generic typed columns: integer_attr1..25, float_attr1..25, text_attr1..25, plus bigint/jsonb/boolean/array/timestamp attrs
- **scenario_scenario_feature** — M2M scenario ↔ scenario_feature
- **workflow** (id uuid) — project_id, name, config (jsonb), is_default
- **report** / **report_layout** (id uuid) — project_id, name, config (jsonb), is_default
- **project_public** — public sharing config: password, config (jsonb snapshot)
- **user_project** — user ↔ project with initial_view_state (jsonb)
- **system_setting** — per-user: client_theme, preferred_language, unit
- **uploaded_asset** — user uploads: s3_key, file_name, mime_type, file_size, asset_type, content_hash
- **cost / credit_usage** — credit metering

## Common Queries

```sql
-- Projects with layer counts
SELECT p.id, p.name, p.created_at, COUNT(lp.id) AS layer_count
FROM customer.project p
LEFT JOIN customer.layer_project lp ON lp.project_id = p.id
GROUP BY p.id ORDER BY p.created_at DESC;

-- Layers in a project with styles
SELECT lp.id, lp.name, lp.order, l.type, l.feature_layer_type, l.feature_layer_geometry_type
FROM customer.layer_project lp
JOIN customer.layer l ON l.id = lp.layer_id
WHERE lp.project_id = 'PROJECT_UUID'
ORDER BY lp.order;

-- Job status
SELECT id, type, status, created_at, payload->>'tool_type' AS tool
FROM customer.job ORDER BY created_at DESC LIMIT 10;

-- Scenario features for a scenario
SELECT sf.id, sf.feature_id, sf.edit_type, ST_AsText(sf.geom) AS geom
FROM customer.scenario_feature sf
JOIN customer.scenario_scenario_feature ssf ON ssf.scenario_feature_id = sf.id
WHERE ssf.scenario_id = 'SCENARIO_UUID';
```

## Important Notes

- Layer **metadata** lives in PostgreSQL (`customer.layer`), layer **data** lives in DuckLake (managed by geoapi)
- `layer_project.properties` = style/rendering config (jsonb); `layer_project.query` = active filters (jsonb)
- `scenario_feature` uses generic columns (integer_attr1..25 etc.) — check `layer.attribute_mapping` for which attr maps to which real column name
- A public dashboard reads the `project_public.config` snapshot, not the live project — re-publish to reflect changes
- Always use READ-ONLY queries. Never INSERT/UPDATE/DELETE unless explicitly asked
- Use `ST_AsText()` or `ST_AsGeoJSON()` to read geometry columns
