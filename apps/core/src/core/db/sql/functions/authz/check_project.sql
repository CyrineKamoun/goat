CREATE OR REPLACE FUNCTION accounts.check_project(
    resource_id_input     UUID,
    user_id_input         UUID,
    organization_id_input UUID,
    project_ids           UUID[]
)
RETURNS BOOLEAN AS $$
DECLARE
    project_role_ids    UUID[];
    project_role_names  TEXT[];
    project_id_loop     UUID;
    status_check        BOOLEAN := FALSE;
    folder_grant_role   TEXT;
    resource_method_arr TEXT[];
BEGIN

    /* Get the needed roles for the project */
    SELECT *
    INTO project_role_ids, project_role_names
    FROM accounts.get_needed_roles(resource_id_input, 'project');

    IF array_length(project_role_ids, 1) = 0 THEN
        RAISE EXCEPTION 'No roles found for the resource';
    END IF;

    /* HTTP methods for this resource — used for viewer/editor distinction */
    SELECT method
    INTO resource_method_arr
    FROM accounts.resource
    WHERE id = resource_id_input;

    FOR i IN 1..array_length(project_ids, 1) LOOP
        project_id_loop := project_ids[i];

        /* 1. User-level grant */
        IF EXISTS (
            SELECT 1
            FROM accounts.project_user ul
            WHERE ul.project_id = project_id_loop
              AND ul.user_id    = user_id_input
              AND ul.role_id    = ANY(project_role_ids)
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 2. Organisation-level grant */
        IF EXISTS (
            SELECT 1
            FROM accounts.project_organization ol
            WHERE ol.project_id      = project_id_loop
              AND ol.organization_id = organization_id_input
              AND ol.role_id         = ANY(project_role_ids)
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 3. Team-level grant */
        IF EXISTS (
            SELECT 1
            FROM accounts.project_team lt
            JOIN accounts.user_team    t  ON lt.team_id = t.team_id
            WHERE lt.project_id = project_id_loop
              AND t.user_id     = user_id_input
              AND lt.role_id    = ANY(project_role_ids)
            LIMIT 1
        ) THEN
            status_check := TRUE;
            CONTINUE;
        END IF;

        /* 4. Folder-level ResourceGrant
         *
         *  If the project lives in a folder shared with a team or organisation
         *  the user belongs to, derive access from the folder role:
         *    folder-editor → full access (any HTTP method)
         *    folder-viewer → read-only (resource must not include
         *                    POST / PUT / DELETE / PATCH)
         */
        SELECT r.name
        INTO   folder_grant_role
        FROM   customer.project         p
        JOIN   accounts.resource_grant  rg
            ON rg.resource_type = 'folder'
           AND rg.resource_id   = p.folder_id
        JOIN   accounts.role            r  ON r.id = rg.role_id
        WHERE  p.id           = project_id_loop
          AND  p.folder_id   IS NOT NULL
          AND  (
               (    rg.grantee_type = 'team'
                AND EXISTS (
                        SELECT 1
                        FROM   accounts.user_team ut
                        WHERE  ut.team_id = rg.grantee_id
                          AND  ut.user_id = user_id_input
                    )
               )
            OR (    rg.grantee_type = 'organization'
                AND EXISTS (
                        SELECT 1
                        FROM   accounts."user" u
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
        RAISE EXCEPTION 'User does not have access to the project';
    END IF;

    RETURN status_check;
END;
$$ LANGUAGE plpgsql;
