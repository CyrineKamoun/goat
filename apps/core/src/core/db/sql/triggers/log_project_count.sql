CREATE OR REPLACE FUNCTION customer.log_project_count()
RETURNS TRIGGER AS $$
DECLARE
    organization_id_input UUID;
    project_count_difference int := 0;  -- Default project count difference
BEGIN
    -- Get the organization_id of the project
    SELECT organization_id
    INTO organization_id_input
    FROM customer.user
    WHERE id = COALESCE(NEW.user_id, OLD.user_id);

    -- Handle INSERT: A new project is added
    IF TG_OP = 'INSERT' THEN
        project_count_difference := 1;

    -- Handle DELETE: A project is removed
    ELSIF TG_OP = 'DELETE' THEN
        project_count_difference := -1;
    END IF;

    -- Update the organization's project count
    UPDATE customer.organization o
    SET used_projects = used_projects + project_count_difference
    WHERE o.id = organization_id_input;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger for INSERT and DELETE
CREATE OR REPLACE TRIGGER log_project_count_trigger
AFTER INSERT OR DELETE
ON customer.project
FOR EACH ROW
EXECUTE FUNCTION customer.log_project_count();
