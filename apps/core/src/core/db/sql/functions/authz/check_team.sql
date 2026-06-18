CREATE OR REPLACE FUNCTION customer.check_team(
    user_id_input UUID,
    team_ids_input UUID[],
    resource_id_input UUID
)
RETURNS BOOLEAN AS $$
DECLARE
    team_role_ids UUID[];
    team_role_names TEXT[];
    team_id_input UUID;
BEGIN
    /* Get the needed team roles */
    SELECT *
    INTO team_role_ids, team_role_names
    FROM customer.get_needed_roles(resource_id_input, 'team');

    /*Check if user has the role for the requested resource for each team */
    FOR team_id_input IN SELECT UNNEST(team_ids_input) LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM customer.user_team tu
            WHERE tu.team_id = team_id_input
            AND tu.user_id = user_id_input
            AND tu.role_id = ANY(team_role_ids)
        ) THEN
            RAISE EXCEPTION 'User is not in team or does not have the needed role';
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;