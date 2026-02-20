# Database Query Skill

Query the GOAT PostgreSQL database to inspect data, debug issues, and understand state.

## Connection

Read credentials from `.env`:
```bash
source /home/p4b/goat/.env
docker exec -e PGPASSWORD=$POSTGRES_PASSWORD goat-db psql -h 127.0.0.1 -U $POSTGRES_USER -d $POSTGRES_DB
```

For one-off queries:
```bash
source /home/p4b/goat/.env && docker exec -e PGPASSWORD=$POSTGRES_PASSWORD goat-db psql -h 127.0.0.1 -U $POSTGRES_USER -d $POSTGRES_DB -c "YOUR SQL HERE"
```

## Schemas

| Schema | Purpose |
|--------|---------|
| `accounts` | Users, organizations, teams, roles |
| `customer` | Core app data: projects, layers, scenarios, jobs, workflows |
| `ducklake` | DuckLake catalog (managed by geoapi, don't modify directly) |

## Key Tables & Relationships

### accounts schema
- **user** (id uuid) — Keycloak-synced users. Fields: firstname, lastname, avatar
- **organization** (id uuid) — name, avatar
- **team** (id uuid) — belongs to org. name, avatar
- **role** (id uuid) — permission roles (name)
- **user_team** — M2M: user ↔ team
- **layer_organization / layer_team** — layer sharing with role
- **project_organization / project_team** — project sharing with role

### customer schema
- **project** (id uuid) — user_id → user, folder_id → folder, active_scenario_id, layer_order[], basemap, tags[]
- **layer** (id uuid) — user_id → user, folder_id → folder, data_store_id → data_store. Key fields: name, type, data_type, tool_type, feature_layer_type, feature_layer_geometry_type, extent (geometry), properties (jsonb), url, size, attribute_mapping (jsonb), in_catalog (bool), tags[]
- **layer_project** (id int) — M2M: layer ↔ project. Fields: name, properties (jsonb, style config), other_properties, query (jsonb, filters), charts, order, layer_project_group_id
- **layer_project_group** (id int) — Layer groups within a project. project_id, parent_id (self-ref for nesting), order
- **data_store** (id uuid) — Storage backends. type field
- **folder** (id uuid) — user_id → user, name
- **job** (id uuid) — user_id → user. type, status, payload (jsonb)
- **scenario** (id uuid) — project_id → project, user_id → user, name
- **scenario_feature** (id uuid) — layer_project_id → layer_project, feature_id (text), edit_type, geom (geometry), h3_3, h3_6. Has 25 sets of generic typed columns: integer_attr1..25, float_attr1..25, text_attr1..25, plus bigint_attr1..5, jsonb_attr1..10, boolean_attr1..10, array attrs, timestamp attrs
- **scenario_scenario_feature** — M2M: scenario ↔ scenario_feature
- **workflow** (id uuid) — project_id → project, name, config (jsonb), is_default
- **report_layout** (id uuid) — project_id → project, name, config (jsonb), is_default, is_predefined
- **user_project** — user ↔ project with initial_view_state (jsonb)
- **project_public** — public sharing config for project. password, config (jsonb)
- **system_setting** — per-user settings: client_theme, preferred_language, unit
- **uploaded_asset** — user file uploads: s3_key, file_name, mime_type, file_size, asset_type, content_hash
- **status** — simple id/status lookup table

## Common Queries

```sql
-- List all projects with layer counts
SELECT p.id, p.name, p.created_at, COUNT(lp.id) as layer_count
FROM customer.project p
LEFT JOIN customer.layer_project lp ON lp.project_id = p.id
GROUP BY p.id ORDER BY p.created_at DESC;

-- Get layers in a project with their styles
SELECT lp.id, lp.name, lp.order, l.type, l.feature_layer_type, l.feature_layer_geometry_type
FROM customer.layer_project lp
JOIN customer.layer l ON l.id = lp.layer_id
WHERE lp.project_id = 'PROJECT_UUID'
ORDER BY lp.order;

-- Check job status
SELECT id, type, status, created_at, payload->>'tool_type' as tool
FROM customer.job ORDER BY created_at DESC LIMIT 10;

-- Scenario features for a scenario
SELECT sf.id, sf.feature_id, sf.edit_type, ST_AsText(sf.geom) as geom
FROM customer.scenario_feature sf
JOIN customer.scenario_scenario_feature ssf ON ssf.scenario_feature_id = sf.id
WHERE ssf.scenario_id = 'SCENARIO_UUID';
```

## Important Notes

- Layer **metadata** lives in PostgreSQL (`customer.layer`), layer **data** lives in DuckLake (managed by geoapi)
- `layer_project.properties` contains the style/rendering config (jsonb)
- `layer_project.query` contains active filters (jsonb)
- `scenario_feature` uses a generic column approach (integer_attr1..25 etc.) — check `layer.attribute_mapping` to understand which attr maps to which real column name
- Always use READ-ONLY queries. Never INSERT/UPDATE/DELETE unless explicitly asked
- Use `ST_AsText()` or `ST_AsGeoJSON()` to read geometry columns
