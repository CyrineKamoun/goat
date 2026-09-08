CREATE OR REPLACE FUNCTION customer.check_project(
    resource_id_input     UUID,
    user_id_input         UUID,
    organization_id_input UUID,  -- kept for the authorization.sql call signature; the user's organisation is read from customer."user" inside effective_role
    project_ids           UUID[]
)
RETURNS BOOLEAN AS $$
DECLARE
    needed_rank INT;
    writes      BOOLEAN;
    project_id  UUID;
BEGIN
    /* The weakest role the resource accepts (roles are alternatives). Every
       project write endpoint requires at least editor, so a resource with
       write methods never accepts a viewer. */
    SELECT MIN(customer.role_rank(n)) INTO needed_rank
    FROM (
        SELECT UNNEST(role_names) AS n FROM customer.get_needed_roles(resource_id_input, 'project')
    ) x;
    IF needed_rank IS NULL OR needed_rank = 0 THEN
        RAISE EXCEPTION 'No roles found for the resource';
    END IF;
    SELECT (method && ARRAY['POST','PUT','DELETE','PATCH']::text[]) INTO writes
    FROM customer.resource WHERE id = resource_id_input;
    IF writes THEN
        needed_rank := GREATEST(needed_rank, 2);
    END IF;

    FOREACH project_id IN ARRAY project_ids LOOP
        IF customer.role_rank(customer.effective_role('project', project_id, user_id_input)) < needed_rank THEN
            RAISE EXCEPTION 'User does not have access to the project';
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
