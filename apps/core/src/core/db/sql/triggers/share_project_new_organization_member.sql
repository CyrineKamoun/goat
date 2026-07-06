CREATE OR REPLACE FUNCTION customer.share_project_new_organization_member()
RETURNS TRIGGER AS $$
BEGIN
    -- Handle case where organization_id is set to NULL
    IF NEW.organization_id IS NULL THEN
        -- Delete the user's records from customer.user_project when organization_id is set to NULL
        DELETE FROM customer.user_project
        WHERE user_id = OLD.id;
    ELSE
        -- Insert the new member into all projects owned by their organization's members
        INSERT INTO customer.user_project(user_id, project_id, initial_view_state, updated_at)
        SELECT NEW.id, j.project_id, j.initial_view_state, now()
		FROM 
		(
		    SELECT p.project_id
		    FROM customer.project_organization p 
		    WHERE p.organization_id = NEW.organization_id
		) p
		CROSS JOIN LATERAL 
		(
		    SELECT up.project_id, up.initial_view_state
		    FROM customer.user_project up
		    JOIN customer.project pp ON up.project_id = pp.id
		    WHERE up.project_id = p.project_id
		    AND up.user_id = pp.user_id
		    AND up.user_id <> NEW.id
		) j
		-- Avoid inserting rows that already exist for the new user
		LEFT JOIN customer.user_project existing
		ON existing.project_id = j.project_id
		AND existing.user_id = NEW.id
		WHERE existing.user_id IS NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER share_project_new_organization_member_trigger
AFTER INSERT OR UPDATE OR DELETE ON customer.user
FOR EACH ROW
EXECUTE FUNCTION customer.share_project_new_organization_member();