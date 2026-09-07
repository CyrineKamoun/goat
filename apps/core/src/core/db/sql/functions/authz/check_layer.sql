CREATE OR REPLACE FUNCTION customer.check_layer(
    resource_id_input     UUID,
    user_id_input         UUID,
    organization_id_input UUID,  -- kept for the authorization.sql call signature; the user's organisation is read from customer."user" inside effective_role
    layer_ids             UUID[]
)
RETURNS BOOLEAN AS $$
DECLARE
    needed_rank INT;
    layer_id    UUID;
BEGIN
    /* The weakest role the resource accepts on the layer side (roles are
       alternatives). Layer-typed roles only: a project-write endpoint that
       also references layer_ids (e.g. adding a layer to a project) only
       needs read access to those layers — the project side is enforced by
       check_project. Fall back to project-typed roles only when the
       resource has no layer-typed permissions at all. */
    SELECT MIN(customer.role_rank(n)) INTO needed_rank
    FROM (SELECT UNNEST(role_names) AS n FROM customer.get_needed_roles(resource_id_input, 'layer')) x;
    IF needed_rank IS NULL OR needed_rank = 0 THEN
        SELECT MIN(customer.role_rank(n)) INTO needed_rank
        FROM (SELECT UNNEST(role_names) AS n FROM customer.get_needed_roles(resource_id_input, 'project')) x;
    END IF;
    IF needed_rank IS NULL OR needed_rank = 0 THEN
        RAISE EXCEPTION 'No roles found for the resource';
    END IF;
    /* No write-method floor here: layer write endpoints (layer/{id} PUT/DELETE
       etc.) are seeded with update-layer/delete-layer permissions, whose
       roles already rank >= editor, so needed_rank already reflects that. */

    FOREACH layer_id IN ARRAY layer_ids LOOP
        IF customer.role_rank(customer.effective_role('layer', layer_id, user_id_input)) < needed_rank THEN
            RAISE EXCEPTION 'User does not have access to the layers';
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;
