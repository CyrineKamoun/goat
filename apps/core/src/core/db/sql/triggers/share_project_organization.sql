CREATE OR REPLACE FUNCTION accounts.share_project_organization()
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
        SELECT p.project_id, t.id, initial_view_state, now()
        FROM customer.user_project p
        JOIN accounts.user t 
        ON t.organization_id = NEW.organization_id
        WHERE p.project_id = project_id_check
        AND p.user_id = owner_id
        AND t.id != owner_id
        ON CONFLICT DO NOTHING;
    -- Remove project_user when a link is deleted and the user is not the owner
    ELSIF TG_OP = 'DELETE' THEN
        DELETE FROM customer.user_project
        WHERE project_id = project_id_check
        AND user_id IN (
            SELECT t.id
            FROM customer.user_project p
            JOIN accounts.user t 
            ON t.organization_id = OLD.organization_id
            WHERE p.project_id = project_id_check
            AND p.user_id = owner_id
            AND t.id != owner_id
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for sharing project when a new project-team link is created
CREATE OR REPLACE TRIGGER share_project_organization_insert_trigger
AFTER INSERT ON accounts.project_organization
FOR EACH ROW
EXECUTE FUNCTION accounts.share_project_organization();

-- Trigger for removing the project-user link when a project-team link is deleted
CREATE OR REPLACE TRIGGER share_project_organization_delete_trigger
AFTER DELETE ON accounts.project_organization
FOR EACH ROW
EXECUTE FUNCTION accounts.share_project_organization();
