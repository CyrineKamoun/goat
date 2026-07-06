CREATE OR REPLACE FUNCTION customer.check_layer(
    resource_id_input     UUID,
    user_id_input         UUID,
    organization_id_input UUID,
    layer_ids             UUID[]
)
RETURNS BOOLEAN AS $$
DECLARE
    needed_role_ids     UUID[];
    needed_role_names   TEXT[];
    layer_id_loop       UUID;
    status_check        BOOLEAN := FALSE;
    folder_grant_role   TEXT;
    resource_method_arr TEXT[];
BEGIN

    /* Get the needed roles for the layer */
    WITH unified_roles AS (
        SELECT UNNEST(COALESCE(role_ids, ARRAY[]::UUID[])) AS role_ids,
               UNNEST(COALESCE(role_names, ARRAY[]::TEXT[])) AS role_names
        FROM customer.get_needed_roles(resource_id_input, 'layer')
        UNION ALL
        SELECT UNNEST(COALESCE(role_ids, ARRAY[]::UUID[])),
               UNNEST(COALESCE(role_names, ARRAY[]::TEXT[]))
        FROM customer.get_needed_roles(resource_id_input, 'project')
    )
    SELECT ARRAY_AGG(role_ids), ARRAY_AGG(role_names)
    INTO   needed_role_ids, needed_role_names
    FROM   unified_roles;

    /* HTTP methods for this resource — used for viewer/editor distinction */
    SELECT method
    INTO resource_method_arr
    FROM customer.resource
    WHERE id = resource_id_input;

    FOR i IN 1..array_length(layer_ids, 1) LOOP
        layer_id_loop := layer_ids[i];

        /* 1. Catalog read */
        IF EXISTS (
            SELECT 1 FROM customer.layer l
            WHERE l.id = layer_id_loop AND l.in_catalog = TRUE
        ) AND 'layer-viewer' = ANY(needed_role_names)
        THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 2. User-level grant */
        IF EXISTS (
            SELECT 1 FROM customer.layer_user ul
            WHERE ul.layer_id = layer_id_loop
              AND ul.user_id  = user_id_input
              AND ul.role_id  = ANY(needed_role_ids)
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 3. Organisation-level grant */
        IF EXISTS (
            SELECT 1 FROM customer.layer_organization ol
            WHERE ol.layer_id       = layer_id_loop
              AND ol.organization_id = organization_id_input
              AND ol.role_id         = ANY(needed_role_ids)
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 4. Team-level grant */
        IF EXISTS (
            SELECT 1
            FROM customer.layer_team lt
            JOIN customer.user_team t ON lt.team_id = t.team_id
            WHERE lt.layer_id = layer_id_loop
              AND t.user_id   = user_id_input
              AND lt.role_id  = ANY(needed_role_ids)
            LIMIT 1
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 5. Project-user grant */
        IF EXISTS (
            SELECT 1
            FROM customer.layer_project pl
            JOIN customer.project_user pu ON pl.project_id = pu.project_id
            WHERE pl.layer_id = layer_id_loop
              AND pu.user_id  = user_id_input
              AND pu.role_id  = ANY(needed_role_ids)
            LIMIT 1
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 6. Project-organisation grant */
        IF EXISTS (
            SELECT 1
            FROM customer.layer_project pl
            JOIN customer.project_organization po ON pl.project_id = po.project_id
            WHERE pl.layer_id       = layer_id_loop
              AND po.organization_id = organization_id_input
              AND po.role_id         = ANY(needed_role_ids)
            LIMIT 1
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 7. Project-team grant */
        IF EXISTS (
            WITH layer_project AS (
                SELECT project_id FROM customer.layer_project pl
                WHERE pl.layer_id = layer_id_loop
            ),
            project_team AS (
                SELECT t.*
                FROM customer.project_team t, layer_project l
                WHERE t.project_id = l.project_id
                  AND t.role_id    = ANY(needed_role_ids)
            )
            SELECT *
            FROM customer.user_team u, project_team t
            WHERE u.team_id = t.team_id
              AND u.user_id = user_id_input
            LIMIT 1
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 8. Folder-level ResourceGrant
         *
         *  If the layer lives in a folder shared with a team or organisation
         *  the user belongs to, derive access from the folder role:
         *    folder-editor → full access (any HTTP method)
         *    folder-viewer → read-only (resource must not include
         *                    POST / PUT / DELETE / PATCH)
         */
        SELECT r.name
        INTO   folder_grant_role
        FROM   customer.layer            l
        JOIN   customer.resource_grant   rg
            ON rg.resource_type = 'folder'
           AND rg.resource_id   = l.folder_id
        JOIN   customer.role             r  ON r.id = rg.role_id
        WHERE  l.id            = layer_id_loop
          AND  l.folder_id    IS NOT NULL
          AND  (
               (    rg.grantee_type = 'team'
                AND EXISTS (
                        SELECT 1
                        FROM   customer.user_team ut
                        WHERE  ut.team_id = rg.grantee_id
                          AND  ut.user_id = user_id_input
                    )
               )
            OR (    rg.grantee_type = 'organization'
                AND EXISTS (
                        SELECT 1
                        FROM   customer."user" u
                        WHERE  u.id              = user_id_input
                          AND  u.organization_id = rg.grantee_id
                    )
               )
          )
        LIMIT 1;

        IF folder_grant_role = 'folder-editor' THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        IF folder_grant_role = 'folder-viewer'
           AND resource_method_arr IS NOT NULL
           AND NOT (resource_method_arr && ARRAY['POST','PUT','DELETE','PATCH']::text[])
        THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

    END LOOP;

    IF status_check = FALSE THEN
        RAISE EXCEPTION 'User does not have access to the layers';
    END IF;

    RETURN status_check;
END;
$$ LANGUAGE plpgsql;
