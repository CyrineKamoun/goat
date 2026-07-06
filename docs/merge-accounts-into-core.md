# Merging `accounts` into `core` — Step-by-Step Guide

## Goal & scope

Fold the `apps/accounts` service into `apps/core` so there is **one backend codebase, one build, one deployment, one Alembic chain** — now that accounts no longer needs to be a separate closed-source repo.

**Decisions that shape this guide (agreed up front):**

1. **Keep the two database schemas** (`accounts` + `customer`) physically as they are. We merge *code*, not data. No table moves, no FK rewrites, no data migration. Collapsing to a single schema is an explicit **non-goal** here and can be done later as a separate effort.
2. **Authorization reconciliation is deferred.** Today there are three overlapping authz mechanisms (the `accounts.authorization()` SQL function, core's legacy `*_team`/`*_organization` link tables, and core's newer `accounts.resource_grant`). This guide preserves all of them as-is. Consolidating them is a follow-up project (see "Deferred follow-ups").

**What "done" means for this guide:** `apps/accounts` no longer exists as a separate runnable service / submodule / Poetry project. Its endpoints, models, CRUD, webhooks (Keycloak + Stripe), SQL functions, triggers, and seeds all live inside `apps/core` (or are mounted by core), run from the uv workspace, deploy from the root `compose.yaml`, and the OSS "accounts disabled" mode still works.

---

## Key facts this plan relies on

(From the deep study — verify these still hold before starting.)

- **One physical database already.** Both services point at the `goat` Postgres DB; the wall is purely schema + which service runs migrations. The accounts `.env` documents that a separate DB is impossible because of circular cross-schema FKs.
- **No HTTP between the two backends.** accounts' `CORE_API_URL` is dead config; core has no `ACCOUNTS_URL`. All coupling is in the shared DB. *Nothing to re-wire at the network layer.*
- **Two Alembic chains, scoped by schema.** core's [alembic/env.py](apps/core/alembic/env.py) filters to `customer` only (version table `alembic_version`); accounts filters to `accounts` only (version table `alembic_version_accounts`). They are non-overlapping — this is what makes unification safe.
- **Duplicated models.** core re-declares `User/Organization/Team/Role` as stub SQLModels pinned to `schema=accounts` with a *subset* of columns; accounts owns the superset. The stubs must converge on the accounts definitions.
- **accounts is Pydantic v1 + Poetry; core is Pydantic v2 + uv.** Porting required.
- **External integrations owned only by accounts:** Keycloak (PhaseTwo) webhook listener + admin client, Stripe webhooks/billing, AWS S3 assets, SMTP email, a `pg_cron` monthly credit reset.

---

## Guiding principles for the whole merge

- **Each phase is independently mergeable and leaves a green build.** Don't create a long-lived branch that's broken for weeks.
- **Move, don't rewrite.** Port files as literally as possible; behavior-changing refactors are separate commits, ideally separate PRs.
- **Preserve the disabled-accounts OSS mode** (`ACCOUNTS_DISABLED` / stubbed user/org data) at every step.
- **The DB does not change in this guide.** If a step seems to require an Alembic migration that alters a table, stop — that's out of scope (we're merging code against the existing schema).

---

## Phase 0 — Prep & safety net

**0.1** Confirm the branch (`feature/merge-accounts-into-core`) is current and clean.

**0.2** Snapshot the Alembic heads so we can prove we didn't change the DB. **Verified 2026-06-09:**
```
core (alembic_version):           c3a1d7f2e9b4  AND  d1e2f3a4b5c6   ← TWO open heads
accounts (alembic_version_accounts): 52b3607097ca                  ← single head
```
> ⚠️ **core has an unmerged branchpoint** at `e8bd0e42b8eb` that split into two still-open heads (`c3a1d7f2e9b4` = organization analytics; `d1e2f3a4b5c6` = custom_basemaps/document_asset mergepoint). This is a pre-existing condition unrelated to accounts, but it means `alembic upgrade head` is ambiguous — use `upgrade heads`, or `alembic merge` the two before relying on a linear chain. The Phase 7 squash makes this moot going forward, but **every live DB must be stamped at BOTH core heads + the accounts head** before squashing.

**0.3** Get accounts running locally against the shared `goat` DB once, end-to-end, so you have a known-good baseline to compare against after each phase (login → list orgs → share a layer → a Stripe/Keycloak webhook hitting the listener).

**0.4** Inventory of `apps/accounts` (verified 2026-06-09). On disk: `src/`, `alembic/` (+`alembic.ini`), `tests/`, `scripts/`, `babel.cfg`, `README.md` — **keep**; `.git` (pointer→`.git/modules/apps/accounts`), `.github/`, `.devcontainer.json`, `.vscode/`, `.pre-commit-config.yaml`, `SECURITY.md`, `Dockerfile`, `docker-compose.yaml`, `.dockerignore`, `Makefile`, `.venv/` — **drop/replace with core's**. Dep files: **both `poetry.lock` and `uv.lock` exist** (partial uv migration already started) plus `pyproject.toml` — reconcile in Phase 2. It is an independent clone of `goat-accounts` (see Phase 1 callout), not a tracked submodule.

**Verify:** both services run; you can reproduce the baseline flows. ✅ before proceeding.

---

## Phase 1 — Keep accounts in place as a reference

**Leave `apps/accounts` exactly as it is for the entire merge.** Do **not** de-submodule, detach, or start tracking it now. We port its code *out* into `apps/core`; the original directory stays as a live, runnable reference and baseline, and is deleted only at the end (Phase 9) once core fully replaces it and is verified.

> **Actual starting state (verified 2026-06-09).** `apps/accounts` is **not** a registered submodule of the monorepo. There is **no `.gitmodules`**, no gitlink in `HEAD`, and the monorepo tracks **0 files** under `apps/accounts` because the path is **gitignored** (`.gitignore:82`). On disk it's an **independent clone** of `git@github.com:plan4better/goat-accounts.git` (its `.git` is a pointer file → `.git/modules/apps/accounts`, leftover from a previously-removed submodule), currently at commit `2f69d11`. Because it's already ignored and untracked, it sits side-by-side with the monorepo without interfering — perfect as a throwaway reference.

**Why keep it until the end:** every later phase copies code into `apps/core/src/core/`. We never need the files *tracked at `apps/accounts`*, so the only thing detaching-now would buy is risk (it's the sole irreversible early action). Keeping the clone in place lets us (a) re-read the original at any point, (b) keep the old service runnable to diff responses against, and (c) decide history-preservation once, at decommission.

**1.1** Nothing to do here beyond noting the two reconciliation facts for later:
   - accounts already contains **both `poetry.lock` and `uv.lock`** (a uv migration was partially started) → reconcile in Phase 2.
   - the repo-level infra to drop (`.git`, `.github/`, `.devcontainer.json`, `.vscode/`, `.pre-commit-config.yaml`, `SECURITY.md`, `Dockerfile`, `docker-compose.yaml`, `Makefile`, `.venv/`) is never copied into core — only `src/`, `alembic/`, `tests/`, `scripts/`, `babel.cfg`, and the SQL asset dirs get ported.

**Verify:** `apps/accounts` is untouched and still runs the baseline flows (Poetry/Pydantic v1) — it remains our reference for the rest of the merge.

---

## Phase 2 — Build: add accounts' runtime deps to core

Goal: `core` can install everything the ported accounts code will need. We do **not** make `apps/accounts` a workspace member (it stays an untracked reference on its own Poetry env). Instead we add accounts' runtime deps that core lacks to `apps/core/pyproject.toml`.

> **Why not a workspace member?** accounts' package is literally named `src` (`from src.config import ...`), and the code's real home is `apps/core/src/core/` (Phase 3). Converting accounts' own `pyproject.toml` to uv just to delete it in Phase 9 is wasted effort. The reference clone keeps running on its existing Poetry/`.venv` for baseline diffs.

**2.1 — Diff the dependency sets.** Core already provides: `fastapi[standard]`, `fastapi-pagination`, `sqlmodel`, `sqlalchemy[asyncio]`, `geoalchemy2`, `asyncpg`, `psycopg`, `shapely`, `geojson`, `boto3`, `python-jose[cryptography]`, `sentry-sdk[fastapi]`, `alembic`, `alembic-utils`. **Accounts-only deps to add to core:** `stripe`, `python-keycloak`, `emails` (SMTP), `babel` (i18n for emails).

> **`phasetwo-sdk` deliberately dropped.** It was only used to **list** and **delete** Keycloak webhooks (`get_webhooks`/`delete_webhook` against `…/realms/{realm}/webhooks`); webhook **creation** already used a raw `requests.post` ([accounts crud/webhook.py:36](apps/accounts/src/crud/webhook.py)), and the registration path is skipped in dev/prod entirely. It's a heavyweight generated OpenAPI client with stale transitive caps. **Phase 4 reimplements those two calls with `httpx`** (~10 lines), so the dep, its git source, and the dependency overrides it required are all gone.

**2.2 — Version reconciliation is a Phase 4 concern, not here.** accounts pins Pydantic v1 / SQLAlchemy 1.4 / sqlmodel 0.0.8 / fastapi 0.96; the ported code runs on core's Pydantic 2.11 / SQLAlchemy 2.0 / sqlmodel 0.0.24 / fastapi 0.115. We do **not** add accounts' old pins — we add only the *new* libraries (above) at versions compatible with core's stack, and the code itself is ported to the new APIs in Phase 4. The ported code won't import-clean until Phase 4; that's expected.

**2.3 — `jwt` → `python-jose`.** accounts uses the `jwt` package; core standardizes on `python-jose` (already present). Don't add `jwt`; rewrite those imports during the Phase 4 port. (Noted, not done here.)

**2.4** Add the new deps to `apps/core/pyproject.toml` `[project].dependencies`, with the `phasetwo-sdk` git pin under root `[tool.uv.sources]`, then `uv lock` to confirm the resolver is happy alongside core's existing stack. (Don't delete `apps/accounts/poetry.lock` — the reference clone still uses it.)

**Verify:** `uv lock` resolves with no conflicts; `uv sync --all-packages` installs the new libs into the workspace venv.

**Done (2026-06-09):** added `stripe`, `python-keycloak`, `emails`, `babel` to [core/pyproject.toml](apps/core/pyproject.toml). `phasetwo-sdk` was evaluated and **dropped** (see callout above) — no git source, no overrides needed. Lock resolves clean (208 pkgs); all four import in core's context.

> **Notes that carry into Phase 4:**
> - **Version jumps** the port must absorb: `python-keycloak 2.16 → 7.1` (admin-client API changed significantly), `stripe 6.7 → 7.14` (minor API drift). The reference clone still runs the old versions, so diff behavior carefully.
> - **Reimplement the two phasetwo webhook calls** (`get_webhooks` list, `delete_webhook`) with `httpx` against `…/realms/{realm}/webhooks[/{id}]`, mirroring the existing raw `requests.post` create.
> - This workspace must be synced with **`uv sync --all-packages`** — plain `uv sync` only syncs the (near-empty) root project and prunes member deps.

---

> **Integration, not relocation (the approach for Phases 3–5).** We do **not** drop accounts in as a `core.accounts` namespace. Each piece is dissolved into its natural home in core and ported as it lands: **models → `core/db/models/`**, **schemas → `core/schemas/`**, **crud → `core/crud/`**, **endpoints → `core/endpoints/v2/` (re-prefixed `/api/v1` → `/api/v2`)**, **deps/utils → `core/deps/` & `core/utils/`**, **SQL assets → core's db bootstrap**. Where accounts duplicates something core already has (User/Org/Team/Role models, `deps/auth.py`, an `organizations` v2 router), we **merge into core's existing file**, not add a parallel one. Porting to Pydantic v2 / SQLAlchemy 2.0 and collapsing duplicates happen *per file as it moves* — they are not separate later passes. The reference clone at `apps/accounts` is the source we copy *from*, by hand, slice by slice.

## Phase 3 — Foundations (shared infra every domain depends on)

Bring the cross-cutting, non-domain pieces into core first, fully ported to v2, since every domain slice in Phase 4 builds on them.

**3.1 — Config.** Merge accounts' `core/config.py` settings into core's [core/core/config.py](apps/core/src/core/core/config.py): Stripe keys, Keycloak admin (`KEYCLOAK_ADMIN`/`_PASSWORD`/`REALM_NAME`), `KEYCLOAK_WEBHOOK_URL`/`_SECRET`, AWS S3-assets, SMTP/email, `CLIENT_URL`. Schema names + Keycloak realm already match — don't duplicate. Drop the dead `CORE_API_URL`. Port v1 `BaseSettings` → pydantic-settings v2 (`SettingsConfigDict`).

**3.2 — Utils.** Move `utils/{token,i18n,email,uuid6,partial,dict,middleware,other}.py` → `core/utils/`, reconciling with any core equivalent (don't add a second `i18n`/middleware if core has one). Bring `locales/`, `templates/email/`, `babel.cfg`. Port to v2.

**3.3 — Deps.** Merge `deps/auth.py` into core's existing [deps/auth.py](apps/core/src/core/deps/auth.py) (core already validates the Keycloak JWT and calls `accounts.authorization()` — keep one implementation; add only what's missing, e.g. `is_superuser`, the password-login bits). Move `deps/{keycloak,stripe,aws,db}.py` → `core/deps/`. In `keycloak.py`, **drop `phasetwo`** — reimplement `get_webhooks`/`delete_webhook` with `httpx` against `…/realms/{realm}/webhooks[/{id}]` (create already uses raw HTTP). Port `python-keycloak` 2.16→7.1 admin-client API.

**3.4 — DB bootstrap & SQL assets.** Move `db/{function,trigger,cron}/*.sql` to core's SQL asset location, and fold `seed_roles`/`seed_costs`/`seed_functions`/`create_triggers` into core's db bootstrap. Fix `seed_functions.py`'s `core.src.db.sql.create_functions` → core's actual `core.db.sql.init_functions` (`FunctionManager`). These get *installed* in Phase 6; here we just relocate + port them.

**Verify:** core imports the merged config/utils/deps clean under Pydantic v2; no `phasetwo` import remains; existing core tests still pass.

**Progress (2026-06-09):**
- **3.1 config — DONE.** Merged accounts settings into [core/core/config.py](apps/core/src/core/core/config.py) (`API_SECRET_KEY`, `API_URL`, `BASIC_SCHEMA`, `KEYCLOAK_ADMIN`/`_PASSWORD`, `KEYCLOAK_WEBHOOK_URL`/`_SECRET`, avatars, SMTP, `GEOAPI_RESOURCES`, `CLIENT_URL`). **Stripe keys made Optional** (was required in accounts) so core still boots in OSS/`ACCOUNTS_DISABLED` mode. Dropped dead `CORE_API_URL`. Verified loads.
- **3.2 utils — DONE.** Moved 9 util files → [core/utils/](apps/core/src/core/utils/) + `locales/` + `templates/email/`; moved `EmailTemplateContent` → [core/schemas/email.py](apps/core/src/core/schemas/email.py). Ported `partial.py` to v2 (`model_fields`/`model_rebuild`); `i18n.py` resolves locale/template dirs from `__file__` (no settings paths); `token.py` already used `jose`. All import + exercised clean.
- **3.3 deps — DONE.** Key finding: core's [deps/auth.py](apps/core/src/core/deps/auth.py) is **already a superset** of accounts' (goatlib `KeycloakAuth`, `is_superuser`, `auth_z`/`auth_z_lite`, `authorization()` call, already on `/api/v2`) → **no merge needed**; moved accounts endpoints will import from `core.deps.auth`. Core's session is **identical** to accounts' (both `isolation_level="AUTOCOMMIT"`) → drop accounts `db.py`/`session.py`, repoint to core's `get_db`. Added [core/deps/stripe.py](apps/core/src/core/deps/stripe.py), [core/deps/aws.py](apps/core/src/core/deps/aws.py) (reuses `settings.S3_CLIENT`), [core/deps/keycloak.py](apps/core/src/core/deps/keycloak.py) (**phasetwo removed**; `keycloak_admin` works as-is on python-keycloak 7.1; `phasetwo_webhook_payload` renamed → `keycloak_webhook_payload`). All import-verified.
- **3.4 SQL bootstrap — folded into later phases.** Core already has a function/trigger install framework ([db/sql/](apps/core/src/core/db/sql/) + `init_functions.py`/`init_triggers.py`). Accounts' `db/{function,trigger,cron}/*.sql` slot into it as part of **Phase 6** install wiring; the `seed_roles`/`seed_costs`/`seed_functions`/`create_triggers` scripts depend on RBAC models, so they move with **Phase 4.1**. Not relocated standalone (would risk wrong placement).

---

## Phase 4 — Integrate the domains, one vertical slice at a time

Each slice moves its **model → schema → crud → endpoint** into core's homes, ported to v2, duplicates collapsed, router mounted under `/api/v2`, then verified against the still-running reference clone before starting the next. Suggested dependency order:

| # | Slice | Notable integration work |
|---|---|---|
| 4.1 | **RBAC models** (`role`, `permission`, `resource`, role/perm/resource link tables) | Collapse `role` with core's [role.py](apps/core/src/core/db/models/role.py) stub. Models + tables + the `authorization()`/`check_*` SQL land in core. *Authz **logic** reconciliation stays deferred — preserve behavior.* |
| 4.2 | **Users** | Collapse accounts' full `User` into core's [user.py](apps/core/src/core/db/models/user.py) stub (accounts is the superset; reconcile columns against the live `accounts.user` table, incl. `organization_id`, `hubspot_id`). `/users` → `core/endpoints/v2`. Includes Keycloak webhook user-sync crud. |
| 4.3 | **Organizations** | Collapse into core's [organization.py](apps/core/src/core/db/models/organization.py) stub. **Merge the `/organizations` router into core's existing v2 organizations endpoints** (which already serve analytics + custom domains). |
| 4.4 | **Teams** | Collapse with core's [team.py](apps/core/src/core/db/models/team.py). `/teams` → v2. |
| 4.5 | **Invitations** | Model + crud → core; endpoints fold into the orgs/teams routers. |
| 4.6 | **Sharing** | ⚠️ Touches the deferred-authz area. Collapse accounts' six `{layer,project}_{user,team,org}` link tables with core's existing `*_team`/`*_organization` + `resource_grant`. **Drop accounts' `layer.py`/`project.py` stubs**; repoint link tables at core's canonical `customer.Layer`/`customer.Project`. `/share` → v2. Preserve behavior; don't redesign authz. |
| 4.7 | **Billing / Stripe** | `stripe` crud/deps; `/billing` → v2. (Stripe webhook lands in 4.9.) |
| 4.8 | **Credits & costs** | `cost`/`credit_usage` models, `seed_costs`, the credit trigger. |
| 4.9 | **Auth + webhooks** | `/auth/access-token` (Keycloak login proxy) reconciled with core's auth; Keycloak + Stripe webhook listeners → v2. |

**Model-layer progress (2026-06-09):** ALL domain models integrated and the full graph `configure_mappers()`-clean:
- `User` collapsed (added `email`/`newsletter_subscribe`/timestamps via `UUIDServerDefaultBase`; union of content rels + `organization`); `Organization` full (30 cols, enums, `@validator`→`@field_validator`); `Team` (+`description`); new `Invitation`, `Cost`, `CreditUsage`. `Quota`/`Plan` enums now canonical in `organization.py` (resource imports them).
- `Job`: **core's existing `customer.job` is canonical** (accounts' was a thinner stub) — not touched.
- `Organization.users` is a plain relationship (org FK is `SET NULL`, not delete-orphan).
- Remaining per slice: **schemas → crud → endpoints (v2) → mount**, ported per checklist below.

**RESUME POINT (code layer).** Targets + known collisions to resolve when porting:
- **schemas — DONE (2026-06-09).** Added accounts schemas to `core/schemas/`: `common.py` (+`Msg`), `user.py` (+UserRead/Create/Update/ProfileUpdate), new `organization.py`/`team.py`/`invitations.py`/`plan.py`/`share.py`/`auth.py`. **Key SQLModel-0.0.24 gotcha:** schemas must NOT inherit a `table=True` model (relationship `Mapped`/`dict` columns break). Fixed by extracting non-table `*Base` mixins — added `UserBase` (user.py) and `TeamBase` (team.py); `OrganizationRead`/`TeamRead`/`UserRead` inherit the `*Base`; `InvitationCreate` is now an explicit `SQLModel`. `TeamRolesEnum` added to team model. All import + `configure_mappers()` clean.
- **crud — DONE (2026-06-09).** Initially brought accounts' `CRUDBase` as `core/crud/base_accounts.py`, then **unified the two CRUDBases into one** [core/crud/base.py](apps/core/src/core/crud/base.py): relaxed its TypeVar bounds from a closed core-model union to generic `SQLModel`/`BaseModel` (works for all models; dropped its imports of specific domain models), folded in accounts' extra methods (`get_by_key`, `get_multi_by_key`, `remove_multi`, `remove_multi_by_key`, `get_n_rows`), kept core's richer `update`/`remove`/`delete`/`update_multi`. Deleted `base_accounts.py`; repointed all 7 importers. (Verified safe: shared methods behaved identically; the only `get_multi` divergence — tuples vs scalars — is in the unpaginated branch, and all accounts callers paginate.) Ported crud: `from_orm`→`model_validate`, `.dict()`→`.model_dump()`, `__fields__`→`model_fields`. Relocated `user`/`organization`/`role`/`user_role`/`team`/`invitation`/`share`/`stripe` with import + `.dict()`→`.model_dump()` rewrites. Rewrote [crud/webhook.py](apps/core/src/core/crud/webhook.py) with **httpx** (phasetwo removed; `get_all`/`delete_by_id`/`delete_all`/`init` take a token; admin token via `keycloak_admin().connection.token` — verify on python-keycloak 7.1 at runtime). Added `OrganizationCreate/Update`, `UserCreate/Update` re-exports to [schemas/__init__.py](apps/core/src/core/schemas/__init__.py). All crud import + `configure_mappers()` clean.
- **endpoints — DONE (2026-06-09).** Ported `auth, billing, organizations, teams, users, share, utils` + `shortcuts.py` (→ `core/endpoints/v2/`) with import/`.dict()` rewrites; dropped accounts' dead `system.py`. Rewrote `webhooks.py` (phasetwo management endpoints → token-based crud via `keycloak_admin().connection.token`; listeners use `keycloak_webhook_payload`/`stripe_webhook_payload`). Mounted all in [v2/api.py](apps/core/src/core/endpoints/v2/api.py) under their prefixes. **Full v2 router builds: 115 routes**, accounts endpoints live alongside core's.
- **Regression fixed:** core had a `core/utils.py` *module* (`optional`, `to_feature_collection`, `build_where`, `sanitize_filename`, …) that the 3.2 `core/utils/` package shadowed → unified by moving `utils.py` content into `core/utils/__init__.py` with accounts' utils as submodules. (Minor: duplicate `optional` now in `__init__` and `partial.py` — harmless, reconcile later.)
- **Phase 4 CODE LAYER COMPLETE.** Whole backend merge imports + `configure_mappers()` clean + router builds.

**MODEL-vs-LIVE-DB GATE: GREEN (2026-06-09).** Ran Alembic `compare_metadata` against the live `goat` DB (`accounts` schema) — **0 diffs**. Drift found and fixed against the real tables:
- `user.hubspot_id`, `organization.hubspot_id` (were missing from models).
- `Team`/`Role` switched to naive-timestamp `UUIDServerDefaultBase` (live is `timestamp without time zone`, not core's `timestamptz`).
- `user.organization_id` FK is **CASCADE** (not `SET NULL`); `user.email` is **NOT NULL**; added `idx_user_organization_id`.
- Re-declared ~40 secondary indexes the live tables have but the ORM models omitted (my 5 new link tables **and** core's pre-existing link tables + `resource_grant`).
- `resource_grant.resource_type`/`grantee_type` → `String(50)` + the 4-col unique constraint + 2 indexes.

This is the Phase 7 empty-autogenerate gate for the `accounts` schema, satisfied now (not deferred). App still builds (122 routes) after all fixes. NOTE: the `customer` schema wasn't compared here (core owns it; may have its own pre-existing drift — check during the squash).
- **Import rewrites** for all moved code: `src.core.config`→`core.core.config`; `src.models[.x]`→`core.db.models[.x]`; `src.schemas`→`core.schemas`; `src.crud`→`core.crud`; `src.utils`→`core.utils`; `src.deps.db`/`src.db.session`→`core.endpoints.deps` (`get_db`); `src.deps.auth`→`core.deps.auth`; `src.deps.{keycloak,stripe,aws}`→`core.deps.*`. Webhook crud: replace phasetwo `get_webhooks`/`delete_webhook` with `httpx`; `phasetwo_webhook_payload`→`keycloak_webhook_payload`. `seed_functions` import → `core.db.sql.init_functions`.

**Per-slice porting checklist** (Pydantic v1→v2, SQLAlchemy 1.4→2.0): `from pydantic import BaseSettings`→`pydantic_settings`; `@validator`→`@field_validator`/`@model_validator`; `Config`→`model_config`; `.dict()`/`.parse_obj()`→`.model_dump()`/`.model_validate()`; SQLAlchemy `Query`/`session.execute` 2.0 style; `obj.dict()` in crud. Mount each router as it's finished, guarded by the existing accounts-enabled flag so OSS mode still degrades gracefully.

**Verify (per slice):** the slice's endpoints serve under `/api/v2` and return data matching the reference clone's `/api/v1` for the same request; no duplicate-table / `extend_existing` errors at import.

### Decision (2026-06-09): link-model reconciliation — core's `_link_model.py` is canonical

Studied both. 7 link tables are defined in both codebases; **core's are more complete**: for customer-schema tables it owns (`layer_project`, `user_project`) core's are far richer (e.g. `layer_project` adds group/order/name/properties/query/charts + relationships vs accounts' bare `id/layer_id/project_id` stub), and the four accounts-schema ACL tables (`layer_organization`, `layer_team`, `project_team`, `project_organization`) are identical in both. Core's are already v2/SQLAlchemy-2.0. **Sole gap:** core's `user_team` was missing `role_id` (accounts has it; the live DB has it via `f1a2b3c4d5e6`) — a stale-model bug, now fixed by adding the column. Reconciliation is therefore **additive + low-risk** (no rewrite of core's working tables), so it does NOT pull deferred-authz *logic* forward:
- **DONE:** added `role_id` to core's `user_team` ([_link_model.py](apps/core/src/core/db/models/_link_model.py)).
- **DONE (additive):** added the 5 link tables core lacked — `role_permission`, `resource_permission`, `user_role`, `layer_user`, `project_user` — column-only (no ORM `Relationship`, to avoid forcing back-populates on User/Role/etc. before their slices).
- **DONE:** added `UUIDServerDefaultBase` (naive-timestamp base matching live `accounts.*`) to [_base_class.py](apps/core/src/core/db/models/_base_class.py); added [permission.py](apps/core/src/core/db/models/permission.py) + [resource.py](apps/core/src/core/db/models/resource.py) (with `RequestMethodEnum`/`QuotaTypeEnum`/`PlanTypeEnum`); added `resource_type` to core's [role.py](apps/core/src/core/db/models/role.py); wired all into [models/__init__.py](apps/core/src/core/db/models/__init__.py). Full models package imports + `configure_mappers()` clean.
- **TODO (resolved 2026-06-09):** accounts' `links_model.py` deleted; crud repointed → core's `_link_model`. Authz `.sql` merged into [db/sql/functions/authz/](apps/core/src/core/db/sql/functions/authz/) + triggers into [db/sql/triggers/](apps/core/src/core/db/sql/triggers/); `seed_roles` made idempotent and wired into `initial_data` (see 7.2 note). `seed_costs` deliberately not wired — credits dead/parked (follow-up 5).
- **Base-class note:** accounts models use **naive-timestamp** (`DateTime`) columns; to keep matching the live `accounts.*` tables (no data migration) the accounts-origin models must preserve naive timestamps — do NOT move them onto core's timestamptz `DateTimeBase`. (`UUIDServerDefaultBase` exists for this.)
- **Quota/Plan enum note:** `QuotaTypeEnum`/`PlanTypeEnum` temporarily defined in `resource.py`; when `organization.py` lands (4.3) reconcile their home (accounts had them in `organization.py`).

---

## Phase 5 — Whole-app reconciliation & verification

After all slices land:

**5.1** Confirm **one definition per physical table** — grep `core/db/models` for any leftover duplicate of `user`/`organization`/`team`/`role`; no `core.accounts` namespace exists.

**5.2** Reconcile **column drift against the live DB**, not the code: the `hubspot_id` columns on `accounts.user`/`organization` (in the init migration, absent from both code copies) and `user.organization_id` — confirm via `\d accounts.user` and make the merged model match reality.

**5.3** Full **Pydantic-v2 import-clean**: `uv run python -c "import core.main"` (or app startup) succeeds with no v1 shims left.

**5.4** `uv run pytest apps/core/tests/` + ported accounts tests green; no `Table already defined` errors.

**Verify:** app boots; users/orgs/teams/share/billing read identically to the reference clone.

---

## Phase 6 — Runtime & deployment unification

By now core *is* the single app (routers mounted in Phase 4). This phase handles the process/deploy side.

**6.1 — Startup lifespan. DONE (2026-06-09).** Accounts' startup only ran `webhook_keycloak.init()` (seeds/triggers/functions were standalone `__main__` scripts, not startup work). Added `webhook_keycloak.init()` to core's lifespan in [main.py](apps/core/src/core/main.py) (try/except; self-skips on dev/prod). Relocated SQL assets → [db/sql/accounts/{functions,triggers,cron}/](apps/core/src/core/db/sql/accounts/) (10 functions, 10 triggers, 1 cron). Relocated + import-ported `seed_roles.py` (12 roles, 78 resource-perm maps) and `seed_costs.py` (18 costs) → `core/db/`. App still builds (122 routes). **SQL function/trigger/cron INSTALL + seed invocation belong in the Phase 7 baseline migration** (the standalone managers `seed_functions`/`create_triggers` are sync `engine.execute`; the squash migration installs via `op.execute` reading these relocated files). **Deployment:** `apps/accounts` Dockerfile/compose left in place (reference clone kept per instruction); root compose env passthrough update is a Phase 9/end item.

**6.2 — Webhooks.** `POST /api/v2/webhooks/{keycloak,stripe}/listener` served by core; update the Keycloak webhook target + Stripe webhook endpoint config to core's host.

**6.3 — Deployment.** Remove `apps/accounts/Dockerfile` + `docker-compose.yaml`; core's image carries the inherited Stripe/Keycloak-admin/AWS/SMTP env. No new network edges (accounts had no inbound deps from core).

**6.4 — Frontend env.** Since endpoints are now **v2**, the frontend's accounts calls move too (Phase 8). In root [compose.yaml](compose.yaml), `NEXT_PUBLIC_ACCOUNTS_API_URL` collapses into `NEXT_PUBLIC_API_URL`.

**Verify:** single `core` process serves everything under `/api/v2`; baseline flows (login, org list, share, both webhook listeners) pass; accounts service no longer runs anywhere.

---

## Phase 7 — Collapse Alembic to a single squashed baseline

Goal: **throw away both migration chains and start from one fresh baseline migration** that builds the entire current schema (both `customer` and `accounts`) from zero. One `versions/` dir, one `alembic_version` table, no two-chain reconciliation.

> **Hard prerequisite — read before doing this.** Squashing only works if **every live database (prod, staging, dev) is already at the current head of BOTH chains.** A squashed baseline cannot upgrade a DB that's behind — it can only be *stamped* onto a DB whose schema already matches. Confirm all environments are at the heads you recorded in Phase 0.2 before proceeding. If any environment is behind, bring it to head with the *old* migrations first, then squash.

**7.1 — Widen core's Alembic to own both schemas. DONE (2026-06-09).** [alembic/env.py](apps/core/alembic/env.py) now uses `MANAGED_SCHEMAS = ["customer", "accounts"]` in both `include_object`/`include_name` (also removed a stray `print(type_)`). **7.2–7.5 (baseline generation, stamping, empty-autogenerate gate) require the live multi-schema DB** — execute at end review per the procedure below; the relocated SQL assets ([db/sql/accounts/](apps/core/src/core/db/sql/accounts/)) + seeds ([db/seed_roles.py](apps/core/src/core/db/seed_roles.py), [db/seed_costs.py](apps/core/src/core/db/seed_costs.py)) are ready for the baseline's `op.execute` install block.

**7.2 — Generate the baseline from a known-good live DB, not from autogenerate alone.**
   Autogenerate captures **tables/columns/indexes/FKs only**. It will **NOT** reproduce:
   - `CREATE SCHEMA` statements (`accounts`, `customer`, `basic`, `user_data`, `temporal`)
   - the SQL **functions** (`accounts.authorization()`, `check_layer`, `check_project`, `check_organization`, `check_team`, `get_needed_roles`, …)
   - the **triggers** (`create_layer`/`create_project` ownership grants, `log_storage`, `log_project_count`, `log_credit_job`, `log_users`, the `share_project_*` fan-out triggers)
   - the **`pg_cron`** job (`reset_organization_credits`)
   - the **seed data** (`seed_roles`, `seed_costs` — the `permission`/`role`/`resource`/`cost` rows the authz engine needs)

   **Implemented differently (2026-06-09):** the baseline migration ([versions/init.py](apps/core/alembic/versions/init.py)) creates **schemas + extensions + tables only** (`SQLModel.metadata.create_all`). Functions, triggers and seeds are NOT in the migration — they're installed idempotently by [scripts/initial_data.py](apps/core/src/core/scripts/initial_data.py) (`init_functions` + `init_triggers` + `seed_roles`), which runs as a deploy step after `alembic upgrade head` (once per deploy — a k8s Job/initContainer, never per-pod at app startup). `seed_roles` was rewritten to be idempotent: roles are insert-missing-only (their ids are FK'd from user assignments), and the permission/resource graph is rebuilt in a **single transaction** (no truncate, no empty-table window for in-flight requests). The cron + `seed_costs` are dead/parked (see follow-up 6).

   So the baseline migration is **autogenerate scaffolding + a large hand-written `op.execute(...)` block** that runs all the SQL asset files relocated in Phase 3. The cleanest construction:
   1. `rm -rf apps/core/alembic/versions/*` (after Phase 7.3's history is exported) and delete accounts' `alembic/` too.
   2. `alembic revision --autogenerate -m "baseline: merged core+accounts schema"` against a DB built by the *old* migrations — gives you the full table DDL for both schemas.
   3. Hand-add, in dependency order, the `CREATE SCHEMA` calls (top of `upgrade()`), then after tables: the function installs, trigger installs, cron job, and seed calls — reusing the same `create_triggers.py` / `seed_*` / `create_functions` code paths from Phase 3 (call them from the migration, or inline the SQL).
   4. Order matters: schemas → tables → FKs → functions → triggers → seeds → cron.

**7.3 — Preserve the old history out-of-band (don't delete blindly).**
   Before removing the old `versions/` files, archive them (tag the pre-squash commit, or move them to `apps/core/alembic/_archive/`). You may need them to debug an environment that turns out not to be at head.

**7.4 — Migrate live environments by stamping.**
   For each existing DB (already at head per the prerequisite):
   ```bash
   uv run alembic stamp head   # points alembic_version at the new baseline; runs NO DDL
   ```
   Drop the old `alembic_version_accounts` table once everything is stamped. **Never run `upgrade` against a populated DB with the new baseline** — it would try to `CREATE TABLE` over existing tables.

**7.5 — Prove the baseline is faithful (the correctness gate).**
   - On a **fresh empty DB**: `alembic upgrade head` must build both schemas, all functions/triggers/cron, and seed rows — then the app's baseline flows (Phase 0.3) must pass.
   - On a **copy of a live DB**: after `alembic stamp head`, `alembic revision --autogenerate` must produce an **empty** migration. Non-empty = the baseline (or the Phase 5 models) drifted from reality — fix until empty.

   **Fresh-DB gate passed (2026-06-09):** scratch DB built solely from `alembic upgrade head` + `python -m core.scripts.initial_data` → 39 tables (0 autogenerate diffs vs models), 14 functions (5 basic helpers later pruned as dead → 9 authz), 13 triggers (9 `accounts` + 4 `customer`), seed graph of 12 roles / 35 permissions / 78 resources / 153 role-permission / 80 resource-permission links. `initial_data` run twice → no duplicates. `accounts.authorization()` round trip on the seeded DB: org-viewer GET org ✓grant, DELETE org ✓deny, GET folder ✓deny; org-editor GET folder ✓grant, DELETE org ✓deny; org-owner DELETE org ✓grant. The live-copy stamp + empty-autogenerate check remains an operator step.

   **Runtime verification passed (2026-06-09):** booted core against the fresh scratch DB and exercised the merged endpoints live. Fixes that came out of it:
   - **Default identity replaces `SAMPLE_AUTHORIZATION`** (decided with MS): the hardcoded expired dev-Keycloak JWT is gone. New `DEFAULT_USER_ID/EMAIL/FIRSTNAME/LASTNAME` + `DEFAULT_ORGANIZATION_NAME` settings; with `AUTH=False` and no bearer, deps return synthetic claims (incl. `superuser` realm role) built from these. [seed_default.py](apps/core/src/core/db/seed_default.py) (run by `initial_data` only when `AUTH=False`) idempotently creates the default user, a `goat_enterprise` org (self-hosted quotas, no trial), the organization-owner role link and a home folder — first boot of a self-hosted instance is fully functional with zero manual steps. `AUTH=True` behavior unchanged.
   - **AUTH=False consistency**: `user_token` now delegates to `get_current_token_claims`; `auth` (raw-token forwarding) no longer demands a header in dev; `is_superuser` no longer KeyErrors on tokens without `realm_access`.
   - **Missing ORM relationships restored**: `User.role_links` ↔ `UserRoleLink.user`/`.role` ↔ `Role.user_links` (used by `get_user_with_roles`, org member listing, invitation accept) — dropped during the model collapse, would have 500'd in production.
   - **Stripe optional**: `create_organization` skips Stripe (customer/subscription/product-metadata) when `STRIPE_SECRET_KEY` is unset and applies `SELF_HOSTED_PLAN_METADATA` (enterprise plan, generous quotas, no trial). `send_email` soft-skips when `SMTP_USER` is unset. Billing-configured deployments behave exactly as before.
   - Schema fixes surfaced live: `UserRead.organization_id` and `Organization.plan_renewal_date` made Optional (legitimately NULL for JIT/self-hosted rows); `/users/organization` returns 404 instead of 500 when the user has no org.
   - **Smoke matrix all 200**: profile, organization, org members, org create/update, teams create/list, invitations create/list, billing plans, folders — first boot, no auth header.
   - **Test suite**: **103 passed, 0 failed** (was 77 passed + 27 pre-existing failures; all fixed 2026-06-09). Merge-related fixture updates: pytest `pythonpath` no longer includes `apps/accounts`; fixtures supply NOT NULL columns the real accounts schema requires (`email`, `role.resource_type`, `user_team.role_id`, full org columns via `tests/utils.make_organization`) and get-or-create the default user instead of blind INSERT (it now exists via seeding/JIT).
   - **Pre-existing test debt cleared (2026-06-09, not merge-caused but fixed on this branch):** (1) 20 DNS/domain tests updated from the removed `_resolve_cname` to the current `_resolve(domain, rdtype)` API via a shared `tests/utils.fake_dns_resolve` factory; the apex-rejection test now asserts apex domains are *accepted* (commit `9295563d3` made apex supported via the A-record path). (2) Two real production bugs fixed in the process: pydantic-v2-invalid `raise ValidationError(...)` in `schemas/layer.py` CRS/KML/geometry validators (would 500 with TypeError instead of 422 on bad input) → `raise ValueError`; and `validate_language_code`/`validate_geographical_code` relied on a `KeyError` that pycountry never raises (returns `None`) → invalid codes were silently accepted. (3) My Content visibility: `get_base_filter` now excludes layers sitting in folders the user doesn't own (unreachable in folder-scoped navigation). (4) Removed the stale `DELETE /user/data-schema` test + conftest call — that route deleted per-user Postgres `user_data` tables, a mechanism retired with the DuckLake move (route removed in v2.3.0).

**Verify:** fresh DB built solely from the single baseline reproduces prod behavior; autogenerate is empty against a stamped live-copy DB; only one `versions/` file and one `alembic_version` table remain.

---

## Phase 8 — Frontend consolidation (MANDATORY)

Because the integration re-prefixes accounts' endpoints from `/api/v1` to **core's `/api/v2`**, the frontend's accounts calls **break unless updated** — this is no longer optional.

**8.1 — DONE (2026-06-09).** Repointed `apps/web/lib/api/{users,organizations,teams,billing,share}.ts` from `NEXT_PUBLIC_ACCOUNTS_API_URL` + `api/v1/*` → `NEXT_PUBLIC_API_URL` + `api/v2/*`. `ACCOUNTS_ENABLED` now keys off `NEXT_PUBLIC_API_URL` (always set) so the OSS stub-fallback path is effectively retired (accounts is now part of core). `NEXT_PUBLIC_ACCOUNTS_API_URL` is now unused → Phase 9. Verify with `pnpm --filter web typecheck` (end review).

**8.2 — DONE (2026-06-10).** Stub-fallback mode retired from `apps/web/lib/api/{users,teams,billing,share}.ts`: removed `ACCOUNTS_ENABLED`, `STUB_USER/ORGANIZATION/TEAM(S)/INVITATIONS/PLANS`, `ACCOUNTS_DISABLED_ERROR`, and the per-hook `fallbackData`/disabled branches — hooks are now plain SWR against `API_BASE_URL`. The stubs were actively harmful: `fallbackData: STUB_ORGANIZATION` (with `phone_number: ""`) was parsed by the org-profile page's Zod schema on first render → unhandled `ZodError` crash even with accounts fully working. OSS mode no longer needs frontend stubs at all — the backend's default identity serves real data. Note: the env-flag `ACCOUNTS_DISABLED` in [constants.ts](apps/web/lib/constants.ts) (UI gating in settings layout / Share modal / folder tree) is a separate switch and was left intact. `NEXT_PUBLIC_ACCOUNTS_API_URL` is now fully unused → remove from envs in 9.3.

**8.3 — Full-stack browser verification (2026-06-10).** Fresh scratch DB → core on :8000 → `next dev` on :3000 (`NEXT_PUBLIC_AUTH_DISABLED=True`, no auth header). Verified in the browser: home dashboard (folder/project/layer + users/profile + users/organization all 200), org profile page (renders the seeded "GOAT" org), org members list, account profile page (default identity GOAT Admin / admin@goat.local), teams page (proper empty state, no stub team), and a **full team-create flow through the UI** (dialog → POST /api/v2/teams 201 → list shows "Smoke Team / Owner"). Two backend fixes came out of it: (1) seeded default org now has non-empty `size`/`phone_number`/`location` (frontend Zod requires min-length 1); (2) **Pydantic v1→v2 trap fixed across merged request schemas** — `field: X | None` without `= None` is *required* in v2 (was optional in accounts' v1), so the frontend's team-create (no `avatar`) got a 422. Defaults added in `TeamCreate`, `OrganizationCreateUpdateBase`, `ShareLayerSchema`, `ShareProjectSchema`, `InvitationOrgUpdate`. This was the risk-register item "Pydantic v1→v2 behavior changes" materializing.

**Verify:** web app exercises all org/team/user/billing/share flows against core's `/api/v2` only; no request still targets `/api/v1`.

---

## Phase 9 — Cleanup & decommission

**Only now do we touch the `apps/accounts` directory.** By this point all its code lives in core, core serves every accounts endpoint, and the baseline flows pass against core alone. The reference clone has done its job.

**Phase 9 — DONE (2026-06-10).** Pre-delete safety check: clone HEAD `2f69d11` matched `origin/goat-accounts`, no unpushed commits, only an untracked `uv.lock` artifact — history fully retained on GitHub. Executed: `rm -rf apps/accounts` + `.git/modules/apps/accounts`; `.gitignore` entry removed; `ACCOUNTS_API_URL`/`NEXT_PUBLIC_ACCOUNTS_API_URL` purged from `.env`, `.env.example`, `turbo.json`, `compose.yaml`, `apps/web/Dockerfile`; CI cleaned (`checks.yml` accounts filter/outputs, `release.yml` disabled accounts image job); `.vscode` launch/tasks entries removed; README env table + license narrative updated (license wording flagged for MS review). **Found & fixed during purge:** [withOrganization.ts](apps/web/middlewares/withOrganization.ts) Edge middleware — the SaaS onboarding/suspension redirect flow — still called the old accounts service (`ACCOUNTS_API_URL` + `api/v1/users`); repointed to server-only `API_URL` + `api/v2/users` (wired through compose as `API_URL: ${NEXT_PUBLIC_API_URL}`, turbo.json, and both env files). Verified after: 103 backend tests pass, web typecheck clean, 110 routes, `uv sync --all-packages` OK. The kept `NEXT_PUBLIC_ACCOUNTS_DISABLED`/`ACCOUNTS_DISABLED` flags remain functional UI/middleware switches.

**9.1 — Tear down the `apps/accounts` reference clone.** Decide history first:
   - **Clean delete (default):** the `goat-accounts` GitHub repo retains all history, so just remove the directory and its leftover gitdir:
     ```bash
     rm -rf apps/accounts
     rm -rf .git/modules/apps/accounts   # leftover submodule gitdir
     ```
   - **Preserve history in the monorepo (only if the team wants it):** instead of deleting, `git subtree add`/merge `goat-accounts` history *before* removing the clone — but since the code now lives under `apps/core`, this is rarely worth it.

**9.2** Stop ignoring the path (so nothing silently shadows a future `apps/accounts` if anyone recreates it): remove `apps/accounts` from `.gitignore:82`. (There's no `.gitmodules` to clean — it never was a registered submodule.)

**9.3** Code cleanup: dead `CORE_API_URL` (already absent from core config), the unmounted `system.py` (dropped during port), and any `ACCOUNTS_*`-only config now folded into core. **Partial (2026-06-09):** `.env.example` updated with `KEYCLOAK_WEBHOOK_*`, `STRIPE_*`, `SMTP_*` (merged-in, optional). `NEXT_PUBLIC_ACCOUNTS_API_URL`/`ACCOUNTS_DISABLED` now unused — can be removed. **Lint — DONE (2026-06-09).** All merged files now pass `ruff check` (`F,E,W,N,I,ANN`). Fixes: import sorting (auto), ~41 `ANN201` return types added as `-> Any` (core allows `ANN401`) on dynamic FastAPI handlers/crud, `*args/**kwargs/db_obj` annotated in `shortcuts.py`, `seed_roles` ctor params, the moved `utils/{dict,middleware,other,token,uuid6}` annotated, `i18n.templateLoader`→`template_loader`, `schemas/auth.eventTypes` got `# noqa: N815` (external Keycloak field). App still builds (122 routes). NOTE: pre-existing core lint debt **outside** the merge (`endpoints/v2/{layer,project}.py`, `scripts/*` — ~16 errors) was left untouched (not introduced by the merge). Also still open: duplicate `optional` in `core/utils/__init__.py` (core's) vs `core/utils/partial.py` (accounts') — pick one.

**9.4** Update [CLAUDE.md](CLAUDE.md), the monorepo README, and `.env.example` (fold `ACCOUNTS_POSTGRES_*` / Stripe / Keycloak-admin / S3-assets / SMTP keys into the documented core set).

**9.5** Update CI to drop the accounts pipeline; ensure ruff/mypy now cover the moved code (note: accounts used different lint config).

**Verify:** full `uv run pytest`, `pnpm typecheck`, `pnpm lint`, `ruff check`, `mypy` all green; clean `docker compose --profile prod up -d` brings up the stack with no accounts service.

---

## Deferred follow-ups (explicitly NOT in this guide)

1. **Authorization consolidation.** Reconcile the three overlapping systems into one:
   - `accounts.authorization()` SQL function + seeded `resource`/`permission`/`role` (route-level)
   - legacy per-resource link tables (`layer_team`, `project_organization`, …) carrying `role_id`
   - the newer generic `accounts.resource_grant` table operated in core's [folder.py](apps/core/src/core/endpoints/v2/folder.py)
2. **Collapse the two schemas into one** (data migration + FK/SQL-function rewrites).
3. **Fix the SQL-injection-shaped `authorization()` call** — both services f-string-interpolate `user_id`/path/method into raw SQL ([core deps/auth.py](apps/core/src/core/deps/auth.py), accounts `deps/auth.py`). Parameterize it.
4. **Reconcile migration drift** beyond what Phase 5 requires (`hubspot_id`, etc.).
5. **Billing: replace Stripe with Odoo via an entitlement contract (direction decided 2026-06-10, MS).** Core should not know any billing provider. The contract core actually needs is tiny — the entitlement state already stored on the org row (`plan_name`, `total_*` quotas, `on_trial`, `plan_renewal_date`, `suspended`), which `authorization()` enforces. Target shape: (a) core exposes one service-protected entitlement endpoint (e.g. `PATCH /organizations/{id}/entitlements`) plus an org-created signal; (b) the billing system (Stripe today, an Odoo connector later) is an external adapter that owns customer/subscription/invoice/trial/dunning lifecycle and pushes entitlement changes in; (c) self-hosted = no adapter ever calls it, orgs keep the env-driven defaults (`DEFAULT_PLAN_NAME` / `DEFAULT_QUOTA_*` in config — wired into `SELF_HOSTED_PLAN_METADATA` 2026-06-10). Trial mechanics and dunning emails move billing-side (Odoo handles natively); quota numbers stop living in Stripe product metadata; `stripe_id` eventually becomes a neutral `billing_customer_ref`. Until the Odoo side exists, Stripe stays contained behind `billing_enabled = bool(STRIPE_SECRET_KEY)` (one crud module, one webhook listener, two branches in `create_organization`) — that guard is the seam the refactor will cut along. Converges with follow-up 6 (credits): goat meters usage, the ERP rates and invoices it.

6. **Credits feature — design decision pending.** The old credit-metering system is now dead end-to-end: consumption (`log_credit_job` trigger) and reset (cron `reset_organization_credits`) were removed because they hung off the dropped `customer.job` table; `seed_costs.py` is never called; `Cost`/`CreditUsage` tables and `organization.used_credits` have no live readers; the billing endpoint is Stripe-only; the frontend `used_credits` field is inert (always 0). The `Cost` actions map ~1:1 to goatlib tools, so the intended future direction is to **define cost per-tool in goatlib** (possibly a formula over feature count/area), with usage-based items like `vector_tile` metered separately — re-keyed to the processes API rather than `customer.job`. Until that's decided: tables/column left in place as dormant scaffolding, nothing pruned, and `seed_costs.py` deliberately **not** wired into `initial_data` (it hardcodes costs in core, opposite of the goatlib direction). Note: the other quota counters (`used_storage`, `used_editors`, `used_viewers`, `used_projects`) are live and unaffected.

---

## Risk register

| Risk | Mitigation |
|---|---|
| Duplicate `Table` definitions collide at import (Phase 5) | Single source of truth per table; run app + tests to flush `extend_existing` issues early |
| Model drift vs. live DB (`hubspot_id`, `organization_id`) | Reconcile against `\d` on a real DB; Phase 7.2 empty-autogenerate is the gate |
| Pydantic v1→v2 behavior changes (Phase 4) | Diff JSON responses against Phase 0 baseline |
| Lost startup side-effects (triggers, seeds, cron, Keycloak webhook reg) | Phase 6.2/6.5 checklist; verify each fires on a fresh boot |
| OSS "accounts disabled" mode breaks | Keep the flag at every phase; CI job that boots core with `ACCOUNTS_DISABLED=True` |
| Webhook endpoints silently stop receiving (Stripe/Keycloak) | Re-point provider config in 6.3; test a real event |
| Squashed baseline run against a live DB → `CREATE TABLE` over existing tables | Stamp, never upgrade, populated DBs (7.4); require all envs at head first (7.1 prereq) |
| Baseline misses non-table objects (functions/triggers/cron/seeds) autogenerate can't see | Hand-write them into the baseline from the Phase 3 SQL assets; empty-autogenerate gate (7.5) |

## Sequencing summary

```
0 Prep/baseline → 1 Keep accounts as reference → 2 Add deps to core
→ 3 Foundations (config/utils/deps/SQL, ported) → 4 Integrate domains slice-by-slice into v2
→ 5 Reconcile & verify whole app → 6 Runtime & deploy unification → 7 Squash Alembic
→ 8 Frontend v1→v2 (mandatory) → 9 Decommission accounts
```

Phases 0–2 are done. Each arrow is a point where the build is green and the work is mergeable; within Phase 4 each *slice* is its own green checkpoint.
