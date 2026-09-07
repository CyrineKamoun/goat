# Open items: flat layer storage, tests, CI

Follow-ups from the flat-layer-storage work (2026-08-16 rehearsal, 2026-08-22
tests/CI pass). The migration runbook is `flat-layer-storage-migration.md`.

## Blocking the catalog backend

All shipped 2026-08-23/24 (option B: plain GeoParquet + `rowid` view; commits
`b669fb1ed..e8dc2919e` on `catalog`, plan doc P1–P4). Nihar's bundles PR #3774
merged on top (`b07cb6fa6` + six fix commits). Remaining catalog work is P5
(locked bundles) and the sections below.

## Manual testing (browser click-throughs) — DONE 2026-08-24

Both flows verified in a real browser session against the local stack. The
catalog flow surfaced and fixed three real bugs (selection double-expansion
404, legend caption clobbering the pending caption, link-frozen
other_properties never clearing pending); the bundle flow surfaced the
missing `GOAT_GEOAPI_HOST` wiring. Verified along the way: pending caption
appears and clears via the 4s poll, ready-before-tiles serves dynamic MVT
(287MB NRW flood layer usable while tippecanoe ran), bundle upload detects
GTFS, imports 7 member layers, and renders the locked drag-disabled group.
Not covered: actual map rendering (headless browser has no WebGL) — worth one
human glance at the map canvas.

## Rollout

- [ ] Dev, then prod, per the runbook. The rule that cost a day locally: the
      goatlib rollout and the data migration must land in the same window, or
      workers compute `lake.user_<uid>.t_<layer>` for tables that have moved
      and every tool run fails.
- [ ] **Chart updates (infra repo, helm values) — new env this branch needs:**
  - tools worker: the five `CATALOG_S3_*` vars (bucket, endpoint, key, secret,
    region — secret via a Secret, not values) and `BUNDLES_DATA_DIR`
    (default `/app/data/bundles` is right when the data volume mounts at
    `/app/data`); append **both** to `WHITELIST_ENVS` or jobs never see them.
  - core: `WINDMILL_URL` / `WINDMILL_TOKEN` / `WINDMILL_WORKSPACE` (enqueues
    materialize directly), plus a **read-only mount of only the catalog
    subtree**: `subPath: catalog`, `readOnly: true`, mounted at
    `/app/data/catalog` (compose equivalent already on the branch;
    `CATALOG_DATA_DIR` then needs no explicit value).
  - geoapi: nothing new if `DATA_DIR` is right (`CATALOG_LAYERS_DIR` derives);
    set explicitly only if the volume layout differs.
  - processes: `BUNDLES_DATA_DIR` consistent with the workers (it resolves
    bundle artifacts for tool runs).
- [ ] **PRE-FLIGHT, before `alembic upgrade head`, per env.** Both must pass.
      1. Nothing is about to be cascade-deleted. `layer.folder_id` is
         `ON DELETE CASCADE`, and the migration NULLs `folder_id` only where
         `catalog_external_uid IS NOT NULL` before dropping the catalog folder.
         Any layer sitting in that folder *without* a backref is destroyed:

         ```sql
         SELECT count(*) FROM customer.layer
         WHERE folder_id = (SELECT f.id FROM customer.folder f
                            JOIN customer."user" u ON u.id = f.user_id
                            WHERE u.email = 'catalog@goat.local')
           AND catalog_external_uid IS NULL;   -- must be 0
         ```
      2. `pg_dump` the three scenario tables if their contents might ever be
         wanted. The migration drops them, and the payload becomes
         uninterpretable anyway once `attribute_mapping` goes with it.

- [ ] **Deploy ordering: `core` cannot be rolling-updated across this
      migration.** It is the only service with an ORM `Layer`/`Project` model,
      so its `select(Layer)` names every column explicitly — an old pod hits
      `UndefinedColumn` on `lineage`, `attribute_mapping`, `data_store_id` and
      `project.active_scenario_id` the instant the migration lands, on every
      layer and project request. Either take a brief core outage (`Recreate`,
      or scale to 0 → migrate → scale up on the new image) or set
      `maxUnavailable=100%` so no old replica survives. geoapi, processes,
      workers and the catalog service are safe: they use asyncpg with explicit
      column lists and never name a dropped column.

- [ ] Per-env DB steps, in order: `alembic upgrade head` (chain is
      `init → 0001_bundles_catalog → 0002_favorites → 0003_sharing_grants →
      0004_spaces → 0005_home_templates`; every step is guarded, so a database
      built by `init` from the current models passes through unchanged and a
      dev/prod-era `init` database gets the full delta — see
      `docs/superpowers/plans/2026-09-06-alembic-consolidation.md`) →
      `initial_data.py` (re-installs `check_layer` with the
      catalog + bundle branches **and `create_layer` with the NULL-owner
      guard**, seeds the `datasets` authz resource row and bundle types).
      `seed_catalog_identity` is gone — there is no catalog user to seed.

      The `create_layer` trigger guard is not optional: without it the
      AFTER INSERT trigger writes `NEW.user_id` into `layer_user.user_id`
      (NOT NULL) and **every first-time catalog promote 500s**. Already-promoted
      items take the UPDATE path and hide it.

- [ ] **Windmill: sync ALL tools, not a subset.** `scenario_id` came off
      `ToolInputBase`, so every published script's signature changed — run
      `python -m goatlib.tools.sync_windmill` and `python -m
      goatlib.tasks.sync_windmill` per env, then verify no registered tool
      still mentions `scenario`. Two hazards:
      * `sync_tool` **deletes before it creates**, and swallows failures. A
        half-finished run leaves tools missing from the workspace with a
        zero exit code. Run it as a pre-deploy job that fails on any
        `status == "failed"`, and check the synced count.
      * A job enqueued with the OLD signature against NEW code does **not**
        error: `scenario_id` lands in `**kwargs` and Pydantic drops it
        (`extra="ignore"`), so the tool returns an answer computed without it.
        Drain the tool queue before syncing.
- [ ] Verify `GOAT_GEOAPI_HOST` is set on core in the charts (it predates this
      branch — folder-delete layer cleanup posts through it — but was missing
      from compose/.env.example until now; unset, bundle imports 503 and
      cleanup is silently skipped).
- [ ] Worker image: the routing extension must be rebuilt WITH the
      `build_timetable` binding or every GTFS bundle imports without its
      timetable artifact (graceful skip, logged as a warning — layers still
      ingest, but PT analysis on the bundle stays unavailable).
- [ ] Catalog service prerequisite: `$DATA_DIR/catalog` mirror files
      (`catalog.parquet` / `mirror_items.parquet` + `nuts.parquet`) must exist
      on the env — the promote endpoint 503s without them (harvester/sync task
      owns producing these).
- [ ] Repoint `/home/p4b/goat/compose.override.yaml` when leaving the `catalog`
      branch — it currently mounts goatlib from the `goat-catalog` worktree, so
      switching branches silently runs stale code in the workers. Backup at
      `compose.override.yaml.bak-preflatten`.

## Bundle follow-ups (from the PR #3774 review; owner: Nihar unless noted)

Fixed at merge time: `POST /datasets` authz seed row, s3_key restricted to the
caller's upload prefix + validation off the event loop, `bundle_id` read-only
on group create, UUID guard before artifact `rmtree`, retryable failed imports
(cleanup + artifact upsert), datasets-grid revalidation after bundle/layer
mutations. Still open:

- [x] ~~`edge_path` / `node_path` in the huff-model job payload~~ — verified
      not exploitable: the runner excludes both fields when building the
      analysis params, so payload values are dropped; only the
      bundle-resolved paths are ever set.
- [ ] No ownership check when a tool run resolves a `bundle_id` artifact —
      but this is PARITY: tool runs don't authorize their input *layer* ids
      either (goatlib logs non-owner access and proceeds). The real fix is
      upstream — authorize resource ids in tool params at job submission
      (processes/core) — one design item covering layers and bundles alike.
- [x] Layer-delete tool now refuses bundle member layers (delete the bundle
      instead); bundle deletion unaffected — it removes the link rows first.
- [x] Bundle folder-moves now require a folder of the *bundle owner*, so a
      shared editor can no longer strand (or cascade-delete) the owner's
      bundle via their own folders.
- [ ] GTFS importer materializes whole files in memory (`shapes.txt` is
      GB-scale in national feeds) → worker OOM; stream via DuckDB instead.
- [ ] Any `.zip` named `*gtfs*`/`*overture*` is forced into the bundle path
      with no way to import as a plain layer — needs an override affordance.
- [ ] `list_bundles` is unpaginated.
- [x] DECIDED 2026-08-24: `enGB` stays — European users expect day-first
      dates even in the English UI.
- [x] DECIDED 2026-08-24: "Datenpaket" is THE German term — all DE strings
      swept (was a three-way split with "Bundle" and "Paket").
- [x] DECIDED 2026-08-24: artifacts stay on the shared data volume
      (`BUNDLES_DATA_DIR`) per the matrices/gtfs mirror precedent — the
      routing engine opens them as local files. The direct-S3 migration must
      absorb them (add to that plan when it starts).

## CI

- [ ] **Decide whether goatlib's integration tests get a CI home.** 18 files,
      currently run nowhere. They are the *only* coverage the analytics tool
      runners have (buffer, join, clip, dissolve, union, centroid,
      intersection, difference, aggregate, layer import/export/delete, project
      export/import). Needs a Postgres service and a DuckLake fixture.
- [ ] **Decide whether the Playwright e2e suite runs in CI.** 7 files, never
      executed. Needs a service stack. The unused Chromium install was removed
      from the web job, so adding e2e means adding it back deliberately.
- [ ] `.github/workflows/` is **not tracked by git in the goat-catalog
      worktree** — confirm where these workflows are versioned before relying
      on the `goatobs` job and the goatlib `unit+utils+io` change.

## With the legacy-catalog retirement (design §11.4)

- [x] **Drop the old catalog's metadata columns from `customer.layer`.**
      DONE 2026-08-27. Thirteen columns removed (migration
      `0001_bundles_catalog`, which also carries the drops below) with
      **nothing replacing them** — the earlier plan to collapse them into a
      JSONB `dataset_metadata` was dropped once it was clear a layer has no
      metadata of its own to hold:

      * a user's uploaded dataset is its name, description and tags; publishing
        one to the catalog will be its own job, not a set of columns;
      * a promoted catalog layer already carries the catalog's record verbatim
        in `other_properties.catalog_item`, and `DatasetSummary` now reads it
        from there, in the catalog's own vocabulary.

      Translating that vocabulary into ours was considered and rejected:
      `DDN2` is an enum invented for the old catalog's filter dropdown, so
      mapping `DL-DE-BY-2.0` onto it replaces a real versioned licence
      identifier with an internal code, loses the `2.0`, and needs a lookup
      maintained forever — for a shape the catalog may change anyway.

      `customer.bundle` keeps its `dataset_metadata` document, because there an
      importer genuinely fills it (`feed_publisher_name` out of a GTFS feed).

      Measured before dropping: 109 of 12,281 layers carried any of the
      thirteen, 66 of them the old `in_catalog` rows.

- [x] **Remove the old catalog.** Endpoints `POST /layer/catalog` and
      `POST /layer/metadata/aggregate`, their schemas (`ICatalogLayerGet`,
      `IMetadataAggregate`, `IMetadataAggregateRead`, `MetadataGroupAttributes`),
      `crud_layer.metadata_aggregate`, the five filter params on
      `LayerGetBase`, and on the web the `CatalogExplorer` modal, its two
      workflow-panel entry points, `useCatalogLayers`, `useMetadataAggregated`
      and `datasetMetadataAggregated`. `METADATA_HEADER_ICONS` moved out of the
      deleted `CatalogDatasetCard` to `lib/constants/metadataIcons.ts`.

- [ ] **Workflow nodes lost their catalog source with the old explorer.**
      `DatasetNodeSettings` and `SqlToolSettings` offered "Catalog Explorer" as a
      data source and nothing replaced it. The new picker (`CatalogBody` +
      `useCatalogFlow`) can serve it, but a workflow node wants a bare
      `customer.layer` id and no project link, so it needs: a promote-only
      endpoint (`promote()` already takes no project — the link is added
      afterwards in `project_layer.py`), a single-select mode in the picker, and
      a decision about materialize — a freshly promoted layer is `pending` until
      the job finishes, and a workflow run against it would find no data.

- [x] **Remove scenarios.** No code referenced them any more: `apps/core` had no
      model, CRUD or endpoint, `apps/web` had nothing at all, and the last live
      reader (`get_scenario_features`) was broken — it treated
      `attribute_mapping` as `{real: generic}` while every stored row is
      `{generic: real}`, so it emitted `sf."category" AS "text_attr1"` against a
      table with no such column. Gone from goatlib: `scenario_id` on
      `ToolInputBase`, `ScenarioSelectorMixin`, `scenario_selector_field`, the
      `SECTION_SCENARIO` UI sections in 22 tools and both catchment-area
      runners, the merge path (`base._merge_scenario_features`,
      `db.get_scenario_features`), the routing payload branch (dead — it tested
      `hasattr(params, "street_network")`, an attribute no params class has),
      and the `scenario` / `scenario_id` i18n keys. The tables go too — see
      below.

      **Rollout step:** re-run `sync_windmill` per environment. Every published
      tool script carried a `scenario_id` argument in its generated `main()`
      signature, because the field lived on `ToolInputBase`. Local Windmill was
      re-synced 2026-08-27 (44 tools + 9 tasks; all 42 registered tools verified
      clean afterwards). Four unregistered leftovers still mention it —
      `catchment_area` and `heatmap_{gravity,connectivity,closest_average}_v2`,
      superseded by renamed tools and never deleted by the syncer — harmless,
      nothing dispatches them, delete when convenient.

- [x] **Scenario tables dropped.** DECIDED 2026-08-27 by the user, knowing there
      are rows in production: *"even if there is scenario data in prod we don't
      need it anymore"*. `customer.scenario` (214 rows, 118 distinct owners),
      `customer.scenario_feature` (246), `customer.scenario_scenario_feature`
      (245) and `customer.project.active_scenario_id` (160 non-null) all go in
      migration `0001_bundles_catalog`, after the column drops so the FKs unwind
      cleanly. The consolidated revisions offer no downgrade (it raises
      `NotImplementedError`); the drop was proven on the dev copy
      (`goat_rehearsal`), where the three tables and the column are gone and
      `alembic check` is clean afterwards.

      Worth knowing if this is ever second-guessed: the payload would have
      become unreadable anyway. `scenario_feature` kept attributes in generic
      slots (`text_attr1`, `jsonb_attr1`, …) and the only thing that said what
      they meant was `layer.attribute_mapping`, which the same migration drops.
      Keeping the tables would have preserved bytes, not information. A
      `pg_dump` of the three tables before running this is the only way back.

- [x] **Drop `attribute_mapping` and the vestigial upload/data-store columns**
      (same migration, `0001_bundles_catalog`). `attribute_mapping` mapped generic physical
      column names back to real ones from the shared-wide-table era; the DuckLake
      migration already applied it (a layer mapping `{"text_attr1": "category"}`
      has a `category` column) and the field list now comes from the table schema
      plus `field_config`. Also dropped: `upload_reference_system` (0 rows) and
      `upload_file_type` (30) — no writer, no read schema — and `data_store_id`
      plus the whole `customer.data_store` table (5 rows, no CRUD, no endpoint,
      no router). Five dead helpers went with it from `core/utils/__init__.py`
      (`get_layer_columns`, `search_value`, `next_column_name`,
      `get_result_column`, `build_insert_query`).

      **Kept deliberately:** `tool_type` (9,828 rows) and `job_id` (11,451).
      Nothing reads either, but they record which tool and which Windmill job
      produced a layer, across most of the table — provenance, not cruft.

- [x] **The layer Metadata modal edits name and description only.** It was still
      posting `dataset_metadata` for layers after the columns went, which would
      have silently dropped every field. The provenance inputs (lineage,
      distribution, licence, attribution, reference year, geographical code) now
      render for **bundles** only, packed with `BUNDLE_METADATA_KEYS`; the form
      is typed on `BundleMetadata`, the widest of the three content kinds it
      serves. `layerMetadataSchema` is now just `contentMetadataSchema`.

- [x] **Second sweep (2026-08-27), after the first pass left residue.** Three
      read-only agents swept core, goatlib and web. What was genuinely caused by
      the removals and is now fixed:

      * `DataLicense` and `validate_geographical_code` still lived in
        `db/models/layer.py` — only bundle provenance uses them, so they moved to
        `schemas/metadata.py` and `pycountry` left the layer model.
      * `seed_roles` still granted `layer/catalog` and `layer/metadata/aggregate`.
      * `CRUDLayer.get_base_filter` kept an `attributes_to_exclude` parameter
        (only `metadata_aggregate` ever passed it) and an `isinstance(params,
        ILayerGet)` guard whose `else` branch existed for the deleted catalog
        schemas — always true now, so both went.
      * `"Create scenarios"` in the three gettext catalogs (`.mo` recompiled).
      * `request_examples` advertised a `get` key, an `export` key for an
        endpoint core does not have, and feature/table create bodies that only
        the raster endpoint would ever see.
      * `export_layer_to_parquet` kept a `project_id` parameter after the
        scenario merge went — **42 call sites across 26 tool files** were still
        passing it, plus the chain through both catchment-area coordinate
        helpers. `db.get_layer_project_id` was orphaned the same way.
      * `analysis/schemas/__init__.py` listed `"vector"` in `__all__` with no
        such symbol, so `import *` raised `AttributeError`.
      * Web: `getDatasetSchema` still declared the five removed filter params,
        `datasetMetadataValue` was the deleted aggregate endpoint's response
        shape, `useLayers` was unsubscribed (leaving three `mutate()` calls in
        rename/delete/share doing nothing — rewired to `matchesContentListKey`),
        `ICON_NAME.SCENARIO`, four dead `AddLayerSourceType` members, eight
        orphaned i18n keys, and stale catalog-explorer comments.
      * **`DatasetSummary` was showing eleven rows that could never resolve** —
        it reads the catalog record but still listed the old layer-metadata
        field names. Rewritten against the snapshot's actual keys
        (`processing:lineage`, `publisher`, `category`), with an explicit
        `i18nKey` per row because the vocabularies differ, and three missing
        `no_metadata_available` strings added in EN and DE.

      Checked and deliberately **not** treated as fallout: the `ARG002` unused
      arguments in `base.py`, `layer_replace.py`, `project_export.py` and
      `project_import.py` are identical to `origin/catalog` — they belong to the
      flat-layer-storage work, not to this removal.

- [x] **Catalog layers have no owner.** DECIDED 2026-08-27: the synthetic
      `catalog@goat.local` user existed only because `layer.user_id` was
      `NOT NULL`, and it dragged a fabricated organization with it (invented
      phone number, industry `other`, region `EU`, 2^31 quotas) plus an
      owner-role link and a folder. `user_id` and `folder_id` are now nullable
      and NULL for every promoted catalog layer, and that whole identity is
      deleted — `CATALOG_USER_ID` / `CATALOG_USER_EMAIL` /
      `CATALOG_ORGANIZATION_NAME` / `CATALOG_FOLDER_ID` and
      `seed_catalog_identity` with it.

      NULL is the truth (a catalog dataset belongs to the provider that
      published it) and every consumer already behaved correctly for it: the
      storage trigger finds no organization and bills nobody, "My Content"
      filters on `user_id = you` and never lists them, `check_layer` grants read
      through `catalog_external_uid IS NOT NULL`, and `canEditLayerFeatures`
      already returns false on a falsy owner.

      The ordering trap, since it would have destroyed data: `layer.folder_id`
      is `ON DELETE CASCADE`, so the rows are detached **before** the folder and
      user are deleted. Dropping the folder first takes the 17 layers with it,
      and all 17 are live in users' projects. (The DB has no FK on
      `layer.user_id` even though the model declares one — pre-existing drift.)
      Verified up **and** down in one rolled-back transaction: 17 layers keep
      their project links, the user and org disappear, no other layer is
      touched, and the legacy `catalog@plan4better.de` account keeps its 66
      `in_catalog` layers.

- [ ] **`in_catalog` is now doing two jobs and should be resolved.** The column
      is the old catalog's flag (66 rows), but the *field name* has been reused
      in the map UI as the read-only marker for catalog layers of either
      generation — `ProjectLayerTree` sets
      `in_catalog: layer.in_catalog || isCatalogLayer(layer)`, and rename,
      delete, the edit table and the layer menu all gate on it.

      Now that catalog layers are unowned, the honest replacement is "I don't
      own this dataset", which `canEditLayerFeatures` already computes. The
      blocker is the 66 legacy rows: they are owned by the real
      `catalog@plan4better.de` login, so ownership alone would make them
      editable, and `check_layer` still grants read through the flag. Options,
      in order of preference: set those 66 to NULL owner too (one UPDATE, then
      `check_layer` keys on `user_id IS NULL OR catalog_external_uid IS NOT
      NULL` and the flag drops), or leave the column as a pure legacy marker.
      Do **not** fabricate `catalog_external_uid` values for them — it is the
      STAC item id and carries promote's idempotency contract.

- [ ] **Decide what identifies a catalog-sourced bundle**, when P5 locked
      bundles land. Today no catalog dataset creates a `customer.bundle` row at
      all: a multi-layer dataset promotes to N layers plus a locked
      `layer_project_group` whose `bundle_id` stays NULL. If catalog bundles
      ever do get a row, they want `catalog_external_uid` + `catalog_version`
      under a partial unique index (the identity contract that gives promote
      its idempotency), plus the STAC snapshot — not the flat provenance
      vocabulary, which no catalog path can fill. Deliberately NOT pre-added:
      "the same bundle at the same version" is undefined while members promote
      as separate layers, and the harvester still publishes no
      `goat:bundleType` and no locked marker (verified against the 2026-08-27
      mirror), so the constraint would be designed blind.

## Found by the post-merge review (2026-08-27)

Second wave, from the adversarial diff review. All fixed unless marked.

- [x] **`customer.bundle` would never have got `dataset_metadata` on an existing
      environment.** The bundle revision `0001_bundles_catalog` was edited in place, but
      it is already applied on dev and locally, so `alembic upgrade head` would
      have succeeded and then every `PUT /bundle/{id}` and every GTFS/Overture
      import would fail on `UndefinedColumn` — and `runner.py` deletes the
      just-ingested member layers when the metadata step fails. The transition
      lives in `0001_bundles_catalog`, guarded on the presence of the eight
      flat columns: add `dataset_metadata`, fold the flat columns into it with
      `jsonb_strip_nulls`, drop them and the unused `properties`. Proven on the
      dev copy (`goat_rehearsal`) and as a no-op on a fresh database.

- [x] **`git checkout --` cost the goatlib translations too.** Same mistake as
      the web locales earlier in the day, and this time I did not notice:
      `5d6d5d8e6` dropped `fields.pt_network_bundle_id` and
      `fields.street_network_bundle_id` in **both** languages, and reverted two
      German renames. Those are live bundle-selector labels in Catchment Area v2
      and Heatmap v2, and `_resolve_property` deletes `label_key` whether or not
      it resolves — so the fields would have rendered as `Pt Network Bundle Id`
      with no description, in both languages. Rebuilt from the pre-change file
      minus only the scenario keys; the net diff is now exactly three keys per
      language.

- [x] **`GET /layer/{id}` silently dropped the catalog record.**
      `other_properties` is typed `ExternalServiceOtherProperties` — a closed
      WMS model — on every layer read schema, so Pydantic stripped
      `catalog_item` and `catalog_materialize` and the dataset detail page had
      nothing to show. Verified with a round-trip returning `{}`. The model now
      allows extra keys; the WMS fields stay typed.

- [x] **The `style(goatlib)` commit un-formatted three files** it claimed to
      format (`tasks/download_s3_folder.py`, `tasks/ducklake_compact.py`,
      `endpoints/v2/layer.py` — the last from the `metadata_aggregate` cut).
      All 82 touched Python files now pass `ruff format --check`.

- [x] **Migration downgrade over-claimed layers.** It re-owned everything
      `WHERE user_id IS NULL OR folder_id IS NULL`, but a real user's layer can
      legitimately sit outside a folder (`get_base_filter` handles exactly
      that), so downgrading would have transferred it to the synthetic catalog
      user — irreversibly, since `user_id` goes back to NOT NULL. Now
      `WHERE catalog_external_uid IS NOT NULL`, matching what upgrade nulled.

- [x] `project_export`'s `layer.get("user_id", params.user_id)` fallback was
      dead — the key is always present now and `None` for a catalog layer.
      Changed to `or`. Harmless today only because `_export_layer_data` ignores
      the argument.

- [x] One value, two labels: the summary tiled `publisher` under the
      "Distributor Name" heading while the list below called it "Publisher".
      Unified on `publisher` (icon added to `METADATA_HEADER_ICONS`), and the
      `email`/`url` render branches went — no field has those types now.

- [x] **The bundle licence is free text now.** It was the same `DDN2`/`CC_BY`
      dropdown we rejected for layers — and it is the one field in the document
      no importer can ever fill (the GTFS extractor says so outright: *"License
      and attribution are not derived. GTFS has no license field."*). So it was
      only ever a human picking the nearest internal code. Now a plain string:
      write `DL-DE-BY-2.0`, the licence the source actually states. Nothing to
      migrate — `bundle.license` is already `TEXT` and every stored value is
      NULL. Gone with it: the `DataLicense` enum, the zod `dataLicense`, the
      `licenseOptions` hook and 11 `metadata.license.*` strings per language.
      `metadata.headings.license` stays — it is still the field's label.

- [x] **The identity-deletion CTE now refuses to take anyone with it.**
      `user.organization_id` is `ON DELETE CASCADE`, so deleting the
      `GOAT Catalog` organization deletes every user in it and all their
      content. The delete is now conditional on the synthetic user being its
      only member; otherwise the organization is left standing. Verified by
      adding a stray human to that org and running the migration: the human
      survives, the org is kept, the synthetic user still goes.

- [x] **Bundle metadata cannot be cleared.** Two independent filters drop an
      emptied field before it can reach the database: `Metadata.tsx` strips
      `""` out of the payload, and `CRUDBundle.update` only merges when the
      document is non-None — so `{"license": ""}` never leaves the browser and
      `{"license": null}` would be ignored if it did. Same as the old
      per-column behaviour, so not a regression, but merge semantics make it
      permanent rather than incidental. Fix needs an explicit clear signal
      (send `null` for a field the user emptied, and have the merge delete keys
      whose value is `null`).


- [x] **`create_layer` trigger 500'd every first-time catalog promote.** The
      AFTER INSERT trigger writes `NEW.user_id` into `layer_user.user_id`
      (NOT NULL), and promoted catalog layers now have none. Guarded with an
      early `RETURN NEW` when the owner is NULL. Proven by promoting a
      never-promoted item ("Velopumpen") for real inside a rolled-back
      transaction: succeeds, 0 `layer_user` grant rows.

      **Why nothing caught it:** the 17 already-promoted layers take the
      `UPDATE … RETURNING id` path, so only a *first* promote hits the INSERT;
      `test_catalog_promote.py` mocks the connection; and the goatlib
      integration conftest builds its own schema with no `layer_user` table and
      no triggers. → `promote()` has no integration coverage against a real
      schema. Worth adding.

- [x] **`create_query_shared_content` inner-joined `User`**, so a layer with no
      owner vanished from every listing built on it, with no error. Benign
      today (those listings are owner-scoped anyway) but a trap for anyone
      surfacing catalog layers later — the open "workflow nodes lost their
      catalog source" item is exactly that. Now a LEFT join, with `get_owned_by`
      returning None instead of a dict of nulls.

- [x] **Migration hardening.** `downgrade()` was not re-runnable (unguarded
      `create_foreign_key` → `DuplicateObject`); and the upgrade now sets
      `lock_timeout = '5s'` — the 17 drops take ACCESS EXCLUSIVE on
      `customer.layer` for the whole alembic transaction, so one long-lived
      reader would have every query on the table queue behind the waiting
      migration.

- [x] **`update_layer_status` writes to a column that does not exist.**
      `goatlib/tools/db.py:623` does `SET total_count = $n`; `customer.layer`
      has no `total_count`. Not a live bug — the method has zero callers — but
      it should go, and it explains the frontend's dead `total_count`:
      `LayerStyle.tsx:608` gates clustering on
      `(activeLayer.total_count ?? 0) <= 100_000`, which is **always true**, so
      the cap has never applied. Use `useProjectLayerFeatureCount` instead (its
      own comment says so) and drop the field from the zod schemas.

- [x] **~4,000 lines of dead frontend**, verified as closed clusters: `DatasetExternal.tsx` kept — see the item below, it is a product call.
      * The pre-Windmill toolbox (~2,400 lines): `Aggregate`, `Join`,
        `OevGueteklassen`, `TripCount` panels — 0 importers each — plus
        `lib/api/tools.ts` (imported only by them) and `lib/api/catchmentArea.ts`.
        They POST to `api/v2/tool/*` and `api/v2/motorized-mobility/*`, which
        core no longer registers. Move `oev-gueteklassen/utils.ts` first — it is
        the one live file in that tree (`OevStationConfigInput.tsx` imports it).
      * `Layer.tsx` (591 lines) where only `LayerVisibilityToggle` (~29) is used.
      * ~20 orphan files (~1,100 lines) incl. `InteractionOptions.tsx` (718),
        `ExportNodeSettings.tsx`, `LayerInfo.tsx`, `ListTile.tsx`, and three
        **zero-byte** tracked files.
      * `apps/docs/openapi.json` — a 153 KB `/api/v1/` snapshot from the
        previous API generation, referenced by nothing.

- [x] **`apps/core/src/core/utils/__init__.py` is ~90% dead** — 22 unreferenced
      functions verified individually. Only `sanitize_filename` and the
      `optional` re-export survive. Removing them also retires `geojson` and
      `rich` from `apps/core/pyproject.toml`; `requests` and `alembic-utils`
      are already unused today.

- [ ] **`DatasetExternal.tsx` is a capability loss, not dead code.** 884 lines,
      0 importers, and the only WMS/WFS/XYZ/COG entry point in the app — while
      the render path still reads `other_properties.legend_urls`. GOAT can
      display external layers it can no longer create. Product decision; keep it
      out of any mechanical sweep.

- [x] **`check_layer` grants a whole batch when one layer passes.**
      `status_check` is set TRUE inside the per-layer loop and never reset.
      Pre-existing, but the new catalog branch makes it easier to hit: one
      unowned catalog layer in a batch authorises the rest. Separate ticket.

## Found by the second review wave (2026-08-27, ten finders + verification)

Everything below was **confirmed by tracing or reproduction**, none of it comes
from today's commits, and only the first two are fixed. Ranked by what a user
would hit first.

### Fixed (commit e1a79023b)

- [x] **`?nuts=` never tested intersection.** Inside the correlated EXISTS the
      bare `geometry` bound to the inner NUTS table, so `ST_Intersects` compared
      the region with itself — always true — and every NUTS search degraded to
      the bbox pre-check. Reproduced on DuckDB 1.5.4; invisible in the fixture
      because its regions are rectangles, for which envelope overlap and
      intersection agree. Fixed by qualifying with the relation; the fixture can
      now carry a concave region and a test asserts the difference.
- [x] **Thumbnail reads shared one DuckDB connection across threadpool
      threads.** 2000 parallel reads of two 2 MB objects: 18 returned the other
      object's bytes, 73 raised. Now a cursor per read, like the two sibling
      readers.

### Catalog layers as tool input — half broken

- [x] **Filtering a catalog layer then running any tool crashes.**
      `base._export_catalog_filtered` reads `filters.clause`; `QueryFilters` has
      only `clauses`. Unconditional `AttributeError`.
- [x] **…and once that is fixed, the filter is silently dropped.** The same
      function passes `json.dumps(cql_filter)` where `build_cql_filter` wants
      `{"filter": …, "lang": "cql2-json"}` (see the correct calls at
      `base.py:1066/1212`); the TypeError is swallowed and an empty filter comes
      back, so a 200-feature filter would buffer the whole 5M-row layer with no
      error. It also omits the geometry-column argument.
- [x] **Exporting a project that contains a catalog layer aborts.**
      `project_export._export_layer_data` does `table_path.split(".", 2)`
      expecting `lake.schema.table`; `resolve_layer_table_path` returns the
      two-part `catalog_layers."t_<id>"` for a catalog layer → `ValueError`,
      whole export fails. Same assumption in `layer_export.py:184/203/289` (a
      single catalog layer export produces a nonsense `DESCRIBE`),
      `layer_delete_multi.py:89` and `finalize_layer.py:213`. Four copies, no
      shared helper — one `split_table_path()` that understands both shapes.
- [x] **A retry loses the catalog view.** `_execute_with_retry` drops
      `self._duckdb_con` on a transient DuckLake error and re-runs on a fresh
      connection where the in-memory `catalog_layers."t_<id>"` view was never
      created → `CatalogException` reported as "layer not found". geoapi solved
      this with connection hooks (`dependencies.py:59-70`); the runner has none.
- [x] **Workflow if-nodes resolve catalog layers to a nonexistent table** —
      `if_node._resolve_layer_sql_ref` lacks the `_catalog_layer_parquet` branch
      the base runner has; the broad excepts swallow it and the workflow silently
      takes the wrong branch.
- [x] `CATALOG_LAYERS_DIR` is honoured by geoapi and hardcoded as
      `DATA_DIR/catalog/layers` in goatlib `base.py:92` and
      `catalog_materialize.py:67`. Set it and geoapi reads a directory nothing
      writes to.

### Materialize lifecycle can strand a layer

- [x] **Deterministic input errors leave `pending` forever.** In
      `catalog_materialize.run`, "no handler for layer type" and "no
      parquet_url" raise *before* `_set_status("running")` and before the inner
      `try` that writes `failed`. The web shows "preparing" indefinitely, polls
      every 4s, and core's heal re-enqueues on `pending` — an infinite loop that
      never reaches the `failed` caption the UI has a string for.
- [x] **A crashed worker leaves `running`, which the heal treats as terminal.**
      `should_enqueue = status in ("pending", "failed")`; nothing writes a
      heartbeat or job id, so an OOM-killed materialize is stuck with no
      operator-free recovery.
- [x] **A tippecanoe failure writes `ready`** (`catalog_materialize.py:364`)
      with `tiles: failed`, so re-adding cannot rebuild the cache and the layer
      serves dynamic tiles at full cost forever.
- [x] **Duplicate enqueue while queued.** A second add during the window
      between `execute_process` returning and the worker flipping `running`
      sees `pending` and enqueues again — two jobs race on the same output file.
- [x] **After materialize finishes, the map never refetches tiles.**
      `Layers.tsx:825` keys the source on `layer.updated_at`, but
      `layer_projects_to_schemas` overwrites it with the *link's* `updated_at`
      (`crud_layer_project.py:57`), which materialize never bumps. The 4s poll
      clears the caption and nothing else; the layer stays blank until a pan or
      reload.
- [x] **Publishing freezes the live status.** `crud_project` builds
      `project_public.config` from `get_layers()` (with the overlay) and stores
      it as JSON — a project published seconds after adding a catalog dataset
      describes it as `pending` forever.
- [x] `pending` is the only status with no `updated_at`, and the failed→pending
      heal `jsonb_set`s the object wholesale, discarding the prior error.

### GC

- [x] **GC vs promote race.** Promote protects a reused layer by bumping
      `updated_at`; GC's guarded DELETE re-checks only `NOT EXISTS (layer_project)`,
      not the grace window. Candidates are selected once, then deleted in a
      loop — a re-add that lands between the two gets its layer (and files)
      deleted, then FK-fails on the link. Add
      `AND updated_at < NOW() - grace` to the DELETE.
- [ ] **Orphaned files.** (GC now skips `running` layers and rows younger than the grace period; the `tmp*/` staging dirs are still unmatched.) GC deletes the row first; a materialize job still
      running then writes the parquet/PMTiles and its status UPDATE matches 0
      rows silently. And a SIGKILL mid-materialize leaves `tmp*/` staging dirs
      (~2× layer size) in the shared layers dir that GC's four exact filenames
      never match.
- [x] geoapi `_catalog_views` is never unregistered — a view for a
      GC-deleted file is replayed onto every new connection forever.

### Correctness elsewhere

- [x] **`check_layer` bundle block picks ONE role with `LIMIT 1` and no
      `ORDER BY`.** A user reaching a bundle as org-viewer *and* team-editor gets
      whichever row the planner returns — intermittent 401s on writes that the
      Python `authorize_bundle` (set-union) allows. The folder block has the same
      shape but is pre-existing.
- [x] **Collection Search compiles `filter=` against the item registry**
      (`stac.py:365`, also `stac_aggregate` and `mcp.search_catalog`) though
      `store.collection_registry` exists — hidden columns leak and the member
      semi-join promotion is bypassed, silently dropping mixed-geometry
      datasets.
- [x] **`tile_service._pmtiles_exists` check-then-get on a `TTLCache`.**
      `not in` and `[]` each re-evaluate expiry, so an entry can pass the test
      and `KeyError` on the read (also thread-unsafe). Use one `.get()`.
- [x] **Mixed layer+bundle listing paginates on a non-unique sort key**
      (`crud_datasets.py:286`) — rows tied on `updated_at` (every bundle import
      produces a block of them) can appear on two pages or none. Add
      `(kind, id)` as tiebreaker.
- [x] **`add_bundle` on a still-importing bundle** commits a locked, empty
      group; the import job's later attach then 409s on the duplicate check.
      Reject `status != ready`.
- [x] **geoapi `_ensure_catalog_view` registers under the lock, creates after
      releasing it** — a second thread sees `known=True` and queries a view that
      does not exist yet → 500 under routine 4-worker concurrency.
- [x] `get_runner_class` picks the first `*ToolRunner` in `dir()` —
      `catchment_area_v2` resolves to the imported v1 `CatchmentAreaToolRunner`
      ('T' < 'V'); ten registry entries resolve to the imported
      `SimpleToolRunner`. Benign only because both catchments say `polygon`.
- [x] Antimeridian bbox (`minx > maxx`, valid per STAC) passes validation and
      compiles to an impossible envelope → 200 with empty results.
- [x] `run_aggregations` resolves facets via the item registry only; a
      collection-only facet → `KeyError` → 500 on schema drift.
- [x] `store.registry`/`store._con` read as two unlocked attributes across a
      reload swap (transient torn state). Everything a reload replaces now lives
      in one `CatalogState`, so the swap is a single rebinding, and the paths
      that compile SQL from the registry and then run it (`search_items`,
      `search_collections`, `run_aggregations`) take `store.snapshot()` once and
      execute on that generation's connection. Pinned by a test that swaps the
      store inside `build_filters` — it fails on the old code. Note the snapshot
      pins the *schema*, not the data: views re-read their files.

### Web (from the last finder)

- [x] **Detail-page stars are local `useState`, not `useFavoriteStars`** —
      favouriting on `/catalog/{id}` is never saved and disagrees with the list
      and the picker. The 'in memory until core keeps them' comments in
      `CatalogDetailView.tsx:36` / `CatalogBody.tsx:35` predate
      `lib/api/favorites.ts`.
- [x] **A failed create wipes the form.** `useCreateFlow.submit`'s `finally`
      calls `reset()` + `onDone()` even when `createEmptyLayer` rejected — 15
      field definitions gone with nothing to retry from.
- [x] **Update-available check capped at 100.** `useCatalogItemVersions` asks
      for `limit: itemIds.length`; the server clamps at 100 and nothing pages,
      so layers beyond the first 100 never show the badge.
- [x] **Concurrent uploads drop each other's job ids** — `setRunningJobIds([...runningJobIds, jobId])`
      uses the array captured at upload start (`useDatasetImport.ts:120`,
      `useCreateFlow.ts:129`); the first upload's completion toast and list
      refresh never fire.
- [x] Uppercase extensions (`EXPORT.CSV`) are refused by a case-sensitive
      check with a hardcoded, truncated English message (`useUploadFlow.ts:195`),
      although the tabular reader lowercases and would handle them.
- [x] Picker `hasMore` compares the deduped list to the server total, so one
      duplicate leaves the skeleton row spinning forever; a bundle checkbox
      click while members load is silently lost; the failed-favourite path is a
      `void mutate` with no `.catch()`.
- [x] **Latent, not live:** `CatalogPickerCard` derives a single-layer id from
      the percent-encoded href, which is encoded again on POST. All 22,965 real
      collection ids are plain UUIDs so it works today; the fixtures' `src:uuid`
      ids would 404. Use `collection.id` directly.

### Labels and docs

- [x] Seven `label_key`s have no translation in either language — `polygon_union`
      (buffer), `egress_mode` (catchment v2), `input_layer_1..3` + `sql_query`
      (custom SQL) — and section `sql` is missing; `_resolve_property` deletes
      `label_key` whether or not it resolves, so they render with **no label at
      all**. `size_field` is top-level in `en.json` but under `fields` in
      `de.json`: German label, no English one.
- [x] Integration tests still assert the retired `lake.user_<uid>` layout
      (`test_layer_import_runner.py:132`, `test_project_export_import.py:686-724`,
      conftest helpers) while production writes to `main` — they fail against
      correct code, and a regression in the `main` write path cannot be caught.
- [x] `ARCHIVE_SKIP_EXTS` (new on this branch) silently skips `.txt`/`.dsv`
      inside a zip; a zipped folder of tab-delimited exports now imports nothing,
      with no message. `_discover_from_zip` now tracks whether anything was
      yielded (across nesting) and raises `DiscoveryError` naming the skipped
      files when they were the only candidates. A README beside real data stays
      silent.
- [ ] Custom SQL workflow node lost its catalog source with the old explorer
      (separate surface from the dataset node already logged).
- [x] `apps/docs` `getting_started/welcome.md:16` and the DE tutorial intro still
      advertise scenarios. `.github/copilot-instructions.md` fixed.

### Efficiency (all in `apps/catalog`)

- [x] All 15 `/stac` handlers are `async def` running synchronous DuckDB
      scans on the event loop — one slow query freezes the service. Make them
      plain `def` or offload to a thread.
- [x] The reload path SHA-256s every served parquet end-to-end on the request
      that wins the lock. **Measured, left alone:** 36 ms over the 60 MB of
      mirror + NUTS, 19% of a 193 ms rebuild, paid by one request per sync and
      no other. A cheaper seed (mtime, sampled bytes) is what the ETag comment
      already rejects — it either differs across replicas or fails to change
      when the converter does.
- [x] `run_aggregations` runs one full filtered scan per facet (N+1) —
      now a single `GROUPING SETS` pass (`_grouped_counts`), with the empty
      grouping set supplying `total_count` for free: 166 ms → 26 ms for the
      default sidebar over the 38k-row mirror. Facet rows are identified by
      per-column `GROUPING()` flags rather than a bitmask, so the mapping does
      not depend on argument order. `record_to_item`/`collection_to_stac` now
      copy shallowly — every edit replaces a top-level member, so the deep walk
      was pure cost.
      **`search_*`'s count+page double scan: measured, rejected.** A single
      `count(*) OVER ()` scan is *slower* than the two queries — 228 ms vs
      110 ms unfiltered, 148 ms vs 155 ms filtered — because the window forces
      the full result set through one operator while `count(*)` alone is served
      from parquet metadata.
- [x] `add_catalog_items_to_project` opens a DuckDB connection and rescans the
      mirror per item, synchronously in the async handler, after
      `resolve_item_ids` already scanned it.

## Test-suite hygiene

- [ ] Three heatmap tests fail on stale local fixture data:
      `data/traveltime_matrices/walking` has `orig_id, dest_id, cost, h3_3`
      while the code reads `traveltime`. They `assert path.exists()` with no
      skip guard. Regenerate the matrices, or add a skip when the schema does
      not match.
- [ ] `apps/geoapi` and `apps/processes` suites fail when a shared `.env` is
      sourced (3 and 2 tests). `apps/catalog` got an autouse fixture that
      clears the env vars its settings read by alias; the same treatment would
      make a root-level run green with `.env` loaded. CI is unaffected — it
      sets only the env each job needs.
- [ ] Cross-app pollution in a single-session run: `apps/core/tests/api/*`
      passes 167/167 alone and fails ~31 when collected with other apps. CI
      never does this (one job per app), so it is a local-workflow issue only.

## Deferred, lower priority

- [ ] `ducklake_compact` SIGSEGV on prod — still unresolved, compact left off.
- [ ] The DuckLake `ATTACH` mkdir race seen once during concurrent thumbnail
      rendering (`[Errno 17] File exists: '/app/data/ducklake'`). A read-only
      manager no longer creates the directory, which may or may not have been
      the cause. If it recurs, capture the **full traceback** — the message
      alone does not name the call site.
- [ ] Lake leftovers: 5 `user_*` directories (238 subdirs, no catalog rows) and
      the `churn_test` / `rowid_test` / `upgrade_test` / `import_probe`
      schemas. Inert; delete when convenient.
