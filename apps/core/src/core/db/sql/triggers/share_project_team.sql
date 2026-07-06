CREATE OR REPLACE FUNCTION customer.share_project_team()
RETURNS TRIGGER AS $$
DECLARE
    owner_id UUID;
   	project_id_check UUID;
BEGIN
	
	-- Get user_id of owner
    IF TG_OP = 'INSERT' THEN
    	project_id_check := NEW.project_id;
    ELSIF TG_OP = 'DELETE' THEN
    	project_id_check := OLD.project_id;
    END IF;
	
    -- Get project owner
    SELECT user_id
    INTO owner_id
    FROM customer.project
    WHERE id = project_id_check;

    -- Insert project_user for each member of the project team when a new link is created
    IF TG_OP = 'INSERT' THEN
        INSERT INTO customer.user_project (project_id, user_id, initial_view_state, updated_at)
        SELECT p.project_id, t.user_id, initial_view_state, now()
        FROM customer.user_project p
        JOIN customer.user_team t ON t.team_id = NEW.team_id
        WHERE p.project_id = project_id_check
        AND p.user_id = owner_id
        AND t.user_id != owner_id
        ON CONFLICT DO NOTHING;

    -- Remove project_user when a link is deleted and the user is not the owner
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM customer.user_project
        WHERE project_id = project_id_check
        AND user_id IN (
            SELECT t.user_id
            FROM customer.user_project p
            JOIN customer.user_team t ON t.team_id = OLD.team_id
            WHERE p.project_id = project_id_check
            AND p.user_id = owner_id
            AND t.user_id != owner_id
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for sharing project when a new project-team link is created
CREATE OR REPLACE TRIGGER share_project_team_insert_trigger
AFTER INSERT ON customer.project_team
FOR EACH ROW
EXECUTE FUNCTION customer.share_project_team();

-- Trigger for removing the project-user link when a project-team link is deleted
CREATE OR REPLACE TRIGGER share_project_team_delete_trigger
AFTER DELETE ON customer.project_team
FOR EACH ROW
EXECUTE FUNCTION customer.share_project_team();