CREATE OR REPLACE FUNCTION customer.role_rank(role_name TEXT)
RETURNS INT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN role_name IS NULL THEN 0
        WHEN role_name = 'owner'  OR role_name LIKE '%-owner'  THEN 3
        WHEN role_name = 'editor' OR role_name LIKE '%-editor' THEN 2
        WHEN role_name = 'viewer' OR role_name LIKE '%-viewer' THEN 1
        ELSE 0
    END
$$;

CREATE OR REPLACE FUNCTION customer.rank_role(rank INT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE rank WHEN 3 THEN 'owner' WHEN 2 THEN 'editor' WHEN 1 THEN 'viewer' ELSE NULL END
$$;

/* Direct grants on one resource for one user, through every grantee kind.
   Capped at rank 2 (editor): a grant row can never mint ownership — rank 3
   is reachable only through the resource's own owner_id column. */
CREATE OR REPLACE FUNCTION customer.direct_grant_rank(resource_type_input TEXT, resource_id_input UUID, user_id_input UUID)
RETURNS INT
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(MAX(LEAST(customer.role_rank(r.name), 2)), 0)
    FROM customer.resource_grant rg
    JOIN customer.role r ON r.id = rg.role_id
    WHERE rg.resource_type = resource_type_input
      AND rg.resource_id   = resource_id_input
      AND (
            (rg.grantee_type = 'user' AND rg.grantee_id = user_id_input)
         OR (rg.grantee_type = 'team' AND EXISTS (
                SELECT 1 FROM customer.user_team ut
                WHERE ut.team_id = rg.grantee_id AND ut.user_id = user_id_input))
         OR (rg.grantee_type = 'organization' AND EXISTS (
                SELECT 1 FROM customer."user" u
                WHERE u.id = user_id_input AND u.organization_id = rg.grantee_id))
      )
$$;

/* Whether a layer is a catalog layer: readable by everyone (including
   anonymous) and writable only through its owner_id column — explicitly
   flagged in_catalog, backed by an external STAC item, or a genuine orphan
   (no space AND no user — the promote-on-use placeholder before it is
   claimed). A layer that lost its user_id (e.g. the creator's account was
   removed) but still belongs to a space is NOT a catalog layer: the space
   still owns it, so offboarding must not turn team-space content into a
   public, write-locked catalog row. Shared by effective_role and
   layer_write_allowed so the predicate is defined once. */
CREATE OR REPLACE FUNCTION customer.layer_is_catalog(layer_id UUID)
RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT (
        l.in_catalog = TRUE
     OR l.catalog_external_uid IS NOT NULL
     OR (l.space_id IS NULL AND l.user_id IS NULL)
    )
    FROM customer.layer l WHERE l.id = layer_id
$$;

/* A folder and its ancestors, nearest first; the depth trigger keeps this to at most 4 rows. */
CREATE OR REPLACE FUNCTION customer.folder_chain(folder_id_input UUID)
RETURNS SETOF UUID
LANGUAGE sql STABLE AS $$
    WITH RECURSIVE chain AS (
        SELECT f.id, f.parent_id, 1 AS depth FROM customer.folder f WHERE f.id = folder_id_input
        UNION ALL
        SELECT p.id, p.parent_id, c.depth + 1 FROM customer.folder p JOIN chain c ON p.id = c.parent_id WHERE c.depth < 4
    )
    SELECT id FROM chain
$$;

/* Restricted (spec D9): the resource itself is marked restricted, or any folder on its chain is,
   or — for a layer — a bundle holding it is (rule 5 propagates a bundle's grants to its member
   layers, so it propagates Restricted too). Suppresses the space-default step only; grants,
   bundle and project paths still apply. An unknown resource type is never restricted: the CASE
   falls through to NULL and COALESCE reads that as FALSE. The five known types are enforced by
   the API (`ResourceType`) and by `authz.require`'s own allow-list, not here.
   `folder_chain(NULL)` is empty, so a resource with no folder needs no guard. */
CREATE OR REPLACE FUNCTION customer.restricted_applies(resource_type_input TEXT, resource_id_input UUID, folder_ref UUID)
RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT COALESCE(
               CASE resource_type_input
                   WHEN 'layer'   THEN (SELECT l.restricted FROM customer.layer l   WHERE l.id = resource_id_input)
                   WHEN 'project' THEN (SELECT p.restricted FROM customer.project p WHERE p.id = resource_id_input)
                   WHEN 'bundle'  THEN (SELECT b.restricted FROM customer.bundle b  WHERE b.id = resource_id_input)
                   WHEN 'folder'  THEN (SELECT f.restricted FROM customer.folder f  WHERE f.id = resource_id_input)
                   WHEN 'template' THEN (SELECT t.restricted FROM customer.template t WHERE t.id = resource_id_input)
                   ELSE NULL
               END, FALSE)
        OR EXISTS (
               SELECT 1 FROM customer.folder_chain(folder_ref) fc
                 JOIN customer.folder f ON f.id = fc
                WHERE f.restricted)
        OR (resource_type_input = 'layer' AND EXISTS (
               SELECT 1 FROM customer.bundle_layer bl
                 JOIN customer.bundle b ON b.id = bl.bundle_id
                WHERE bl.layer_id = resource_id_input
                  AND (b.restricted
                       OR EXISTS (SELECT 1 FROM customer.folder_chain(b.folder_id) bfc
                                    JOIN customer.folder bf ON bf.id = bfc
                                   WHERE bf.restricted))))
$$;

/* THE rule (spec §3.2). Space owner/admin > space default for members > direct grant > ancestor-folder
   grants > bundle grant > project→layer read > catalog read. Max role wins; the one exception is
   Restricted (D9), which withholds the space default. A soft-deleted item is visible to the space
   owner/admin only. Returns 'owner' | 'editor' | 'viewer' | NULL. */
CREATE OR REPLACE FUNCTION customer.effective_role(resource_type_input TEXT, resource_id_input UUID, user_id_input UUID)
RETURNS TEXT
LANGUAGE plpgsql STABLE AS $$
DECLARE
    best        INT := 0;
    space_ref   UUID;
    folder_ref  UUID;
    deleted     TIMESTAMPTZ;
    is_catalog  BOOLEAN := FALSE;
    space_level INT := 0;
BEGIN
    IF resource_type_input = 'layer' THEN
        SELECT l.space_id, l.folder_id, l.deleted_at INTO space_ref, folder_ref, deleted FROM customer.layer l WHERE l.id = resource_id_input;
        IF FOUND THEN
            is_catalog := customer.layer_is_catalog(resource_id_input);
        END IF;
    ELSIF resource_type_input = 'project' THEN
        SELECT p.space_id, p.folder_id, p.deleted_at INTO space_ref, folder_ref, deleted FROM customer.project p WHERE p.id = resource_id_input;
    ELSIF resource_type_input = 'bundle' THEN
        SELECT b.space_id, b.folder_id, b.deleted_at INTO space_ref, folder_ref, deleted FROM customer.bundle b WHERE b.id = resource_id_input;
    ELSIF resource_type_input = 'folder' THEN
        SELECT f.space_id, f.parent_id, f.deleted_at INTO space_ref, folder_ref, deleted FROM customer.folder f WHERE f.id = resource_id_input;
    ELSIF resource_type_input = 'template' THEN
        SELECT t.space_id, t.folder_id, t.deleted_at INTO space_ref, folder_ref, deleted FROM customer.template t WHERE t.id = resource_id_input;
    ELSE
        RAISE EXCEPTION 'effective_role: unknown resource_type %', resource_type_input;
    END IF;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    /* 1–2. the space: owner/admin → owner, member → the space default */
    IF space_ref IS NOT NULL AND user_id_input IS NOT NULL THEN
        space_level := customer.space_rank(space_ref, user_id_input);
    END IF;

    /* trash: only the space owner/admin keeps access (to restore) */
    IF deleted IS NOT NULL THEN
        RETURN CASE WHEN space_level = 3 THEN 'owner' ELSE NULL END;
    END IF;

    IF space_level = 3 THEN
        RETURN 'owner';
    END IF;

    /* catalog layers: readable by everyone, including anonymous; never more than viewer for non-owners */
    IF is_catalog THEN
        RETURN 'viewer';
    END IF;

    IF user_id_input IS NULL THEN
        RETURN NULL;
    END IF;

    /* 2 (Restricted, D9): a restricted item/folder does not hand members the space default */
    IF space_level IN (1, 2) AND customer.restricted_applies(resource_type_input, resource_id_input, folder_ref) THEN
        best := 0;
    ELSE
        best := space_level;
    END IF;

    /* 3. direct grants on the resource itself */
    best := GREATEST(best, customer.direct_grant_rank(resource_type_input, resource_id_input, user_id_input));

    /* 4. grants on the folder and its ancestors (depth ≤ 3) */
    IF folder_ref IS NOT NULL THEN
        SELECT GREATEST(best, COALESCE(MAX(customer.direct_grant_rank('folder', fc, user_id_input)), 0))
          INTO best
          FROM customer.folder_chain(folder_ref) fc;
    END IF;

    IF resource_type_input = 'layer' THEN
        /* 5. bundle inheritance: a member layer is never shared on its own */
        SELECT GREATEST(best, COALESCE(MAX(customer.direct_grant_rank('bundle', bl.bundle_id, user_id_input)), 0))
          INTO best
          FROM customer.bundle_layer bl
         WHERE bl.layer_id = resource_id_input;

        /* 6. project → layer: any access to a containing project grants READ on the layer,
              but only through a SHAREABLE link (D7). A link is non-shareable when whoever
              added the layer to the project lacked `share` on it, so adding a dataset you
              may only view cannot hand the project's other members access to it. */
        IF best < 1 AND EXISTS (
            SELECT 1 FROM customer.layer_project lp
             WHERE lp.layer_id = resource_id_input
               AND lp.shareable
               AND customer.effective_role('project', lp.project_id, user_id_input) IS NOT NULL
        ) THEN
            best := 1;
        END IF;
    END IF;

    /* 7. the GOAT catalog shelf (T4): a published template is readable by every
          authenticated user regardless of the space it lives in. A fallback only:
          a member's own space role, a grant or a folder grant above still wins,
          so a team editor keeps editing its published template. Anonymous callers
          returned NULL above and never reach the shelf. */
    IF best = 0 AND resource_type_input = 'template' AND EXISTS (
        SELECT 1 FROM customer.template t
         WHERE t.id = resource_id_input AND t.catalog_status = 'published' AND t.deleted_at IS NULL
    ) THEN
        best := 1;
    END IF;

    RETURN customer.rank_role(best);
END;
$$;

CREATE OR REPLACE FUNCTION customer.can(resource_type_input TEXT, resource_id_input UUID, user_id_input UUID, action TEXT)
RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT customer.role_rank(customer.effective_role(resource_type_input, resource_id_input, user_id_input)) >=
        CASE action
            WHEN 'read'   THEN 1
            WHEN 'write'  THEN 2
            WHEN 'share'  THEN 2
            WHEN 'delete' THEN 3
            ELSE 99
        END
$$;
