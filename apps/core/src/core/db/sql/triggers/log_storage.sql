CREATE OR REPLACE FUNCTION customer.log_storage_usage()
RETURNS TRIGGER AS $$
DECLARE
    organization_id_input UUID;
    size_difference float := 0;  -- Default size difference
BEGIN
    -- Get the organization_id of the user
    SELECT organization_id
    INTO organization_id_input
    FROM customer.user
    WHERE id = COALESCE(NEW.user_id, OLD.user_id);

    -- Handle INSERT: Only NEW.SIZE is relevant
    IF TG_OP = 'INSERT' THEN
        size_difference := COALESCE(NEW.size, 0) / 1048576::float;

    -- Handle DELETE: Only OLD.SIZE is relevant
    ELSIF TG_OP = 'DELETE' THEN
        size_difference := -COALESCE(OLD.size, 0) / 1048576::float;

    -- Handle UPDATE: Both OLD.SIZE and NEW.SIZE are relevant
    ELSIF TG_OP = 'UPDATE' THEN
        size_difference := (COALESCE(NEW.size, 0) - COALESCE(OLD.size, 0)) / 1048576::float;
    END IF;

    -- Update the organization's used_storage
    UPDATE customer.organization o
    SET used_storage = used_storage + size_difference
    WHERE o.id = organization_id_input;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- Drop the existing update trigger
DROP TRIGGER IF EXISTS log_storage_usage_trigger ON customer.layer;

-- Create a single trigger for INSERT, UPDATE, and DELETE
CREATE OR REPLACE TRIGGER log_storage_usage_trigger
AFTER INSERT OR UPDATE OR DELETE
ON customer.layer
FOR EACH ROW
EXECUTE FUNCTION customer.log_storage_usage();
