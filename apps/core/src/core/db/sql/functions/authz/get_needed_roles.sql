CREATE OR REPLACE FUNCTION customer.get_needed_roles(
    resource_id_input UUID,
    resource_type_input TEXT
)
RETURNS TABLE(
    role_ids UUID[],
    role_names TEXT[]
) AS $$
BEGIN
    /* Get the needed roles for the layer */
    RETURN QUERY
    WITH needed_permissions AS (
        SELECT rp.permission_id
        FROM customer.resource r
        JOIN customer.resource_permission rp  
        ON r.id = rp.resource_id
        WHERE r.id = resource_id_input
    ),
    layer_role_ids AS (
        SELECT rp.role_id
        FROM needed_permissions np
        JOIN customer.role_permission rp
        ON np.permission_id = rp.permission_id
    )
    SELECT ARRAY_AGG(role_id) AS layer_role_ids, ARRAY_AGG(name) AS layer_role_names
    FROM customer.role r
    JOIN layer_role_ids ri
    ON r.id = ri.role_id
    WHERE r.resource_type = resource_type_input;
END;
$$ LANGUAGE plpgsql;