CREATE OR REPLACE FUNCTION customer.share_project_new_team_member()
RETURNS TRIGGER AS $$
DECLARE
    user_organization_id UUID;
BEGIN
    
    -- Get the organization_id of the user
    SELECT organization_id
    INTO user_organization_id
    FROM customer.user
    WHERE id = OLD.user_id;

    -- DELETE FROM customer.user_project for all project that are shared with the team
    IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN

        WITH candidates_to_delete AS (
            SELECT p.project_id
            FROM customer.project_team p, customer.team t
            WHERE team_id = OLD.team_id
            AND p.team_id = t.id
        ),
        unmatched_user AS (
            -- Make sure not in project_user and not in project_organization
            SELECT c.project_id
            FROM candidates_to_delete c
            LEFT JOIN customer.project_user up
            ON up.project_id = c.project_id
            AND up.user_id = OLD.user_id
            WHERE up.project_id IS NULL
        ),
        to_delete AS (
            SELECT u.project_id
            FROM unmatched_user u
            LEFT JOIN customer.project_organization po
            ON po.project_id = u.project_id
            AND po.organization_id = user_organization_id
            WHERE po.project_id IS NULL
        )
        DELETE FROM customer.user_project p
        USING to_delete d
        WHERE p.project_id = d.project_id
        AND p.user_id = OLD.user_id;
    END IF;
    
    -- Insert the customer.user_project for all project that are shared with the team
    IF TG_OP = 'UPDATE' OR TG_OP = 'INSERT' THEN
        INSERT INTO customer.user_project(user_id, project_id, initial_view_state, updated_at)
        SELECT NEW.user_id, j.project_id, j.initial_view_state, now()
        FROM
        (
            SELECT p.project_id 
            FROM customer.project_team p, customer.team t
            WHERE team_id = NEW.team_id
            AND p.team_id = t.id
        ) p
        CROSS JOIN LATERAL
        (
            SELECT up.project_id, up.initial_view_state
            FROM customer.user_project up
            JOIN customer.project pp ON up.project_id = pp.id
            WHERE up.project_id = p.project_id
            AND up.user_id = pp.user_id
            AND up.user_id <> NEW.user_id
        ) j
        -- Avoid inserting rows that already exist for the new user
        LEFT JOIN customer.user_project existing
        ON existing.project_id = j.project_id
        AND existing.user_id = NEW.user_id
        WHERE existing.user_id IS NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER share_project_new_team_member_trigger
AFTER INSERT OR UPDATE OR DELETE ON customer.user_team
FOR EACH ROW
EXECUTE FUNCTION customer.share_project_new_team_member();
