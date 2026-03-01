# Windmill Skill

Context for working with GOAT's Windmill instance. Use the Windmill MCP tools for API calls; this skill provides domain knowledge.

## Setup

- **URL**: http://localhost:8110
- **Workspace**: `goat`
- **Token**: read from `WINDMILL_TOKEN` in `.env`
- **Web UI**: http://localhost:8110

## Folder Structure

All scripts live under `f/goat/`:
- `f/goat/tools/` — Analytics tools (synced from goatlib, run by `processes` service)
- `f/goat/tasks/` — Background tasks (thumbnails, S3 sync, etc.)

## Scripts (synced from goatlib)

Scripts are auto-generated from Python tool classes via `python -m goatlib.tools.sync_windmill`. Source of truth is `packages/python/goatlib/src/goatlib/tools/registry.py`.

### Geoprocessing Tools
| Script | Description |
|--------|-------------|
| `f/goat/tools/buffer` | Buffer polygons around features |
| `f/goat/tools/clip` | Extract features within clip geometry |
| `f/goat/tools/centroid` | Point features at geometric center |
| `f/goat/tools/intersection` | Geometric intersection of two layers |
| `f/goat/tools/dissolve` | Merge polygons by attribute with statistics |
| `f/goat/tools/union` | Geometric union of two layers |
| `f/goat/tools/difference` | Erase — remove overlapping portions |

### Data Management Tools
| Script | Description |
|--------|-------------|
| `f/goat/tools/join` | Spatial and attribute joins |
| `f/goat/tools/custom_sql` | Custom SQL on workflow layers |

### Geoanalysis Tools
| Script | Description |
|--------|-------------|
| `f/goat/tools/origin_destination` | OD lines/points from geometry and matrix |
| `f/goat/tools/aggregate_points` | Aggregate points onto polygons/H3 grids |
| `f/goat/tools/aggregate_polygon` | Aggregate polygons onto polygons/H3 grids |
| `f/goat/tools/geocoding` | Geocode addresses via Pelias |
| `f/goat/tools/clustering_zones` | Spatially contiguous balanced clusters |

### Accessibility Indicators
| Script | Description |
|--------|-------------|
| `f/goat/tools/catchment_area` | Isochrones for walking, cycling, transit, car |
| `f/goat/tools/heatmap_gravity` | Gravity-based accessibility |
| `f/goat/tools/heatmap_closest_average` | Average distance/time to N closest destinations |
| `f/goat/tools/heatmap_connectivity` | Total reachable area within travel cost |
| `f/goat/tools/oev_gueteklassen` | Public transport quality classes (Swiss ARE method) |
| `f/goat/tools/trip_count` | PT trip counts per station |

### Data/Internal Tools (hidden from toolbox UI)
| Script | Description |
|--------|-------------|
| `f/goat/tools/layer_import` | Import from S3/WFS into DuckLake |
| `f/goat/tools/layer_delete` | Delete layer from DuckLake + PostgreSQL |
| `f/goat/tools/layer_delete_multi` | Bulk delete layers |
| `f/goat/tools/layer_update` | Update layer from S3/WFS |
| `f/goat/tools/layer_export` | Export to GPKG/GeoJSON/CSV |
| `f/goat/tools/print_report` | Generate PDF/PNG reports (worker_tag: "print") |
| `f/goat/tools/finalize_layer` | Finalize workflow output layer |
| `f/goat/tools/workflow_runner` | Execute full workflows |

### Background Tasks
| Script | Description |
|--------|-------------|
| `f/goat/tasks/download_s3_folder` | Download S3 folder |
| `f/goat/tasks/generate_thumbnails` | Generate project/layer thumbnails |
| `f/goat/tasks/sync_pmtiles` | Sync PMTiles for tile serving |

## How Tools Execute

1. Frontend calls `processes` service API to start a tool
2. `processes` creates a job in `customer.job` table and submits to Windmill
3. Windmill runs the script (Python) which imports from `goatlib.tools`
4. Tool class (`BaseToolRunner` subclass) reads input from DuckLake, processes, writes output to DuckLake
5. Job status is tracked in `customer.job` (pending → running → finished/failed)

## Syncing Tools to Windmill

After modifying a tool in `goatlib/tools/`:
```bash
WINDMILL_TOKEN=6YS6ijY7xYuZ9IJMghb9Fk7KbOBo2UnK WINDMILL_URL=http://localhost:8110 uv run python -m goatlib.tools.sync_windmill
```

## Checking Jobs

Via Windmill MCP or via database:
```sql
SELECT id, type, status, created_at, payload->>'tool_type' as tool
FROM customer.job ORDER BY created_at DESC LIMIT 10;
```

## Key Code Paths

- Tool registry: `packages/python/goatlib/src/goatlib/tools/registry.py`
- Tool base class: `packages/python/goatlib/src/goatlib/tools/base.py`
- Individual tools: `packages/python/goatlib/src/goatlib/tools/<name>.py`
- Windmill sync: `packages/python/goatlib/src/goatlib/tools/sync_windmill.py`
- Code generation: `packages/python/goatlib/src/goatlib/tools/codegen.py`
- Processes API: `apps/processes/` (OGC API Processes service)
