# Data Editing Feature — Implementation Summary

## Overview

This document describes the **Data Editing** feature implementation for the GOAT WebGIS platform. The feature enables users to edit geospatial and non-geospatial layer data directly from an in-app data table, including editing cell values, adding/deleting rows, and managing columns.

The implementation spans **backend write endpoints** (GeoAPI / FastAPI) and a **frontend editable data table** (Next.js / React / MUI).

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Frontend (Next.js)                                         │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  DataPanel (resizable bottom panel)                 │    │
│  │  ┌───────────────────────────────────────────────┐  │    │
│  │  │  EditableDataTable                            │  │    │
│  │  │  - Inline cell editing                        │  │    │
│  │  │  - Dirty state tracking                       │  │    │
│  │  │  - Server-side pagination & sorting           │  │    │
│  │  │  - Checkbox selection / bulk delete            │  │    │
│  │  │  - Column resize                              │  │    │
│  │  └───────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────┘    │
│                          │                                   │
│               API calls (layers.ts)                          │
│       createFeature / updateFeatureProperties /              │
│       deleteFeaturesBulk / addColumn / renameColumn / etc.   │
└──────────────────────────┬──────────────────────────────────┘
                           │  HTTP (POST/PATCH/PUT/DELETE)
┌──────────────────────────▼──────────────────────────────────┐
│  Backend (GeoAPI — FastAPI)                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  features_write router                              │    │
│  │  - Auth (JWT) + ownership check                     │    │
│  │  - Cache invalidation (Redis tiles + metadata)      │    │
│  │  ┌───────────────────────────────────────────────┐  │    │
│  │  │  FeatureWriteService                          │  │    │
│  │  │  - Feature CRUD (create/update/replace/delete)│  │    │
│  │  │  - Column management (add/rename/delete)      │  │    │
│  │  └──────────────────┬────────────────────────────┘  │    │
│  │                     │                                │    │
│  │  ducklake_write_manager (read_only=False)            │    │
│  │  (threading.Lock serializes all writes)              │    │
│  └─────────────────────┬───────────────────────────────┘    │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    DuckDB / DuckLake
```

**Key design decisions:**
- Separate **read-only** and **read-write** DuckLake managers to avoid contention. The existing read-only manager continues to serve feature reads and tile generation while a new write manager handles mutations.
- DuckDB's **single-writer** constraint is handled by `BaseDuckLakeManager`'s internal `threading.Lock`, which serializes all write operations.
- **Redis tile cache** and **in-memory metadata cache** are both invalidated after every write operation to ensure map tiles and column definitions stay current.

---

## Files Created

### Backend (Python — GeoAPI)

#### 1. `apps/geoapi/src/geoapi/ducklake_write.py`
Write-capable DuckLake manager singleton. Uses `BaseDuckLakeManager(read_only=False)` from goatlib.

```python
from goatlib.storage import BaseDuckLakeManager
ducklake_write_manager = BaseDuckLakeManager(read_only=False)
```

#### 2. `apps/geoapi/src/geoapi/models/write.py`
Pydantic models for all write operations:

| Model | Purpose |
|-------|---------|
| `FeatureCreate` | GeoJSON Feature for creation (geometry + properties) |
| `FeatureUpdate` | Partial properties update |
| `FeatureReplace` | Full feature replacement (geometry + properties) |
| `BulkFeatureCreate` | Batch creation (FeatureCollection) |
| `BulkDeleteRequest` | Batch deletion by ID list |
| `FeatureWriteResponse` | Response with created/updated feature ID |
| `BulkWriteResponse` | Response with list of IDs + count |
| `DeleteResponse` | Response with deleted feature ID |
| `BulkDeleteResponse` | Response with deletion count |
| `ColumnCreate` | Create column (name, type, optional default) |
| `ColumnUpdate` | Rename column (new_name) |
| `ColumnResponse` | Response for column operations |

Also defines `COLUMN_TYPE_MAP` mapping user-friendly types to DuckDB types:
- `string`/`text` → `VARCHAR`
- `integer`/`int` → `INTEGER`
- `bigint` → `BIGINT`
- `number`/`float`/`double` → `DOUBLE`
- `decimal` → `DECIMAL`
- `boolean`/`bool` → `BOOLEAN`
- `date` → `DATE`
- `timestamp` → `TIMESTAMP`
- `json` → `JSON`

Column names are validated with pattern `^[a-zA-Z_][a-zA-Z0-9_]*$`.

#### 3. `apps/geoapi/src/geoapi/services/feature_write_service.py`
`FeatureWriteService` class with these methods:

**Feature CRUD:**
| Method | Description |
|--------|-------------|
| `create_feature()` | INSERT with ST_GeomFromGeoJSON for geometry, generates UUID id |
| `create_features_bulk()` | Batch INSERT in single connection context |
| `update_feature_properties()` | Partial UPDATE — only known, non-protected columns |
| `replace_feature()` | Full UPDATE including geometry |
| `delete_feature()` | DELETE by id with existence check |
| `delete_features_bulk()` | DELETE WHERE id IN (...) |

**Column Management:**
| Method | Description |
|--------|-------------|
| `get_column_names()` | Query information_schema.columns |
| `add_column()` | ALTER TABLE ADD COLUMN |
| `rename_column()` | ALTER TABLE RENAME COLUMN |
| `delete_column()` | ALTER TABLE DROP COLUMN |

Protected columns: `{"id", "geometry", "geom", "rowid"}` — cannot be modified by users.

All writes use `ducklake_write_manager.connection()` with parameterized queries to prevent SQL injection.

#### 4. `apps/geoapi/src/geoapi/routers/features_write.py`
FastAPI router with OGC API Features Part 4-style write endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/collections/{collectionId}/items` | Create feature(s) — accepts `FeatureCreate` or `BulkFeatureCreate` |
| `PATCH` | `/collections/{collectionId}/items/{itemId}` | Update properties (partial) |
| `PUT` | `/collections/{collectionId}/items/{itemId}` | Replace feature (full) |
| `DELETE` | `/collections/{collectionId}/items/{itemId}` | Delete single feature |
| `POST` | `/collections/{collectionId}/items/delete` | Bulk delete features |
| `POST` | `/collections/{collectionId}/columns` | Add column |
| `PATCH` | `/collections/{collectionId}/columns/{columnName}` | Rename column |
| `DELETE` | `/collections/{collectionId}/columns/{columnName}` | Delete column |

**Security:**
- All endpoints require authentication (`get_user_id` dependency)
- `_get_authorized_metadata()` verifies user owns the layer (`metadata.user_id`)

**Cache invalidation:**
- `_invalidate_caches()` clears both Redis tile cache (`invalidate_layer_cache`) and in-memory metadata TTL cache (`_metadata_cache`)

### Frontend (TypeScript — Next.js)

#### 5. `apps/web/components/map/panels/DataPanel.tsx`
Resizable bottom panel component (following the existing `WorkflowDataPanel` pattern):

- **Layout**: Flexbox column, positioned below the map viewport
- **Resizable**: Drag handle with `ns-resize` cursor, document-level mouse event listeners
- **Constants**: `MIN_PANEL_HEIGHT=200`, `DEFAULT_PANEL_HEIGHT=350`, `MAX_PANEL_HEIGHT=700`, `COLLAPSED_HEIGHT=44`
- **Collapse/expand**: Toggle button in header, smooth height transition (disabled during drag)
- **Layer-aware**: Reads `activeLayerId` from Redux store, finds matching `ProjectLayer`, renders `EditableDataTable`
- **Auto-hide**: Returns `null` when no layer is active

#### 6. `apps/web/components/map/panels/EditableDataTable.tsx`
Fully custom editable table built with MUI Table components:

**Data Loading:**
- Server-side pagination via `useDatasetCollectionItems` SWR hook
- `TablePagination` component with options: 10, 25, 50, 100 rows per page
- Sorting via `TableSortLabel` headers (uses geoapi `sortby` param, `-` prefix for desc)

**Inline Cell Editing:**
- Click cell → `TextField` appears (autoFocus), blur/Enter → save to dirty map, Escape → cancel
- Type-aware: number fields use `type="number"` on TextField
- Parses values back to correct types (number, null for empty strings)

**Dirty State Tracking:**
- `Map<string, DirtyCell>` keyed by `${rowId}:${column}`
- Each DirtyCell stores: `rowId`, `column`, `originalValue`, `newValue`
- Visual feedback: dirty rows get yellow background (`rgba(255, 193, 7, 0.08)`), dirty cells get stronger highlight (`rgba(255, 193, 7, 0.12)`)
- Changes are only persisted on explicit "Save" click

**Toolbar Actions:**
| Button | Action |
|--------|--------|
| Add Row | Creates empty feature via `createFeature` API |
| Delete Selected | Bulk deletes via `deleteFeaturesBulk` API |
| Discard | Clears dirty state map |
| Save | Groups dirty cells by row, calls `updateFeatureProperties` per row |

**Other Features:**
- Checkbox column for row selection (select all / individual)
- Column resize (mousedown + document mousemove pattern, same as builder Table.tsx)
- Sticky header and sticky checkbox column
- Resets state (page, selection, dirty cells) when layer changes

---

## Files Modified

### Backend

#### 7. `apps/geoapi/src/geoapi/models/__init__.py`
Added imports and `__all__` exports for all write models from `geoapi.models.write`.

#### 8. `apps/geoapi/src/geoapi/routers/__init__.py`
Added `from geoapi.routers.features_write import router as features_write_router` and added to `__all__`.

#### 9. `apps/geoapi/src/geoapi/main.py`
- Imported `ducklake_write_manager` and `features_write_router`
- Added `ducklake_write_manager.init(settings)` in lifespan startup
- Added `ducklake_write_manager.close()` in lifespan shutdown
- Added `"PUT"` and `"PATCH"` to CORS `allow_methods`
- Added `app.include_router(features_write_router)`

### Frontend

#### 10. `apps/web/lib/api/layers.ts`
Added 9 API functions:

**Feature CRUD:**
- `createFeature(layerId, feature)` → POST
- `createFeaturesBulk(layerId, features)` → POST (FeatureCollection)
- `updateFeatureProperties(layerId, featureId, properties)` → PATCH
- `replaceFeature(layerId, featureId, feature)` → PUT
- `deleteFeature(layerId, featureId)` → DELETE
- `deleteFeaturesBulk(layerId, featureIds)` → POST `/items/delete`

**Column Management:**
- `addColumn(layerId, name, type, defaultValue?)` → POST `/columns`
- `renameColumn(layerId, columnName, newName)` → PATCH `/columns/{name}`
- `deleteColumn(layerId, columnName)` → DELETE `/columns/{name}`

All functions use `apiRequestAuth` with `COLLECTIONS_API_BASE_URL`.

#### 11. `apps/web/app/map/[projectId]/page.tsx`
- Added `import DataPanel`
- Restructured the map container: MapViewer wrapper gets `flex: 1, minHeight: 0`, DataPanel sits below it
- DataPanel only renders when `mapMode === "data"`

#### 12. `apps/web/i18n/locales/en/common.json` & `apps/web/i18n/locales/de/common.json`
Added i18n keys for both English and German:

| Key | English | German |
|-----|---------|--------|
| `add_row` | Add Row | Zeile hinzufügen |
| `changes_saved` | Changes saved | Änderungen gespeichert |
| `delete_selected` | Delete Selected | Ausgewählte löschen |
| `discard` | Discard | Verwerfen |
| `error_adding_row` | Error adding row | Fehler beim Hinzufügen der Zeile |
| `error_deleting_rows` | Error deleting rows | Fehler beim Löschen der Zeilen |
| `error_saving_changes` | Error saving changes | Fehler beim Speichern der Änderungen |
| `row_added` | Row added | Zeile hinzugefügt |
| `rows_deleted` | Rows deleted | Zeilen gelöscht |
| `unsaved_changes` | Unsaved changes | Ungespeicherte Änderungen |

---

## Build Status

- **TypeScript**: Passes `tsc --noEmit` without errors
- **Python (ruff)**: Passes `ruff check` without errors

---

## What's NOT Yet Implemented (Remaining Phases)

### Phase 4: Edit Fields Dialog (Column Management UI)
- Dialog to list all columns with name, type, and label
- Inline rename columns
- Add new columns (name, type, default value)
- Delete columns with confirmation
- "Calculate field" functionality
- Matches the design mockups provided (Edit fields screenshots)

### Phase 5: Map-Based Feature Editing
- Click a feature on the map → edit its properties in a popup/side panel
- Draw new features (point, line, polygon) using MapLibre GL Draw
- Delete features from the map context menu
- Geometry modification (move vertices, reshape)

### Phase 6: Additional Enhancements (Optional)
- Undo/redo stack for cell edits
- Copy/paste cells
- Filter/search within the data table
- Export edited data
- Conflict detection for concurrent edits
- Keyboard navigation between cells (Tab/Arrow keys)
