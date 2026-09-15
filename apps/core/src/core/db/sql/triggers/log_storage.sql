-- Storage accounting for an organization, maintained from its layers.
--
-- SECURITY DEFINER on purpose: the function is owned by the application role,
-- which may write `customer.organization`, while callers that hold only a
-- narrow grant on `customer.layer` may not. The thumbnail task is the case
-- that forced this — it connects as the read-only role, holds a column grant
-- on `layer.thumbnail_url`, and every write it made aborted with
-- "permission denied for table organization" raised by this trigger. Book-
-- keeping a row the caller cannot see is the definer's job, not the caller's.
-- `search_path` is pinned because a definer function inherits the caller's
-- otherwise.
CREATE OR REPLACE FUNCTION customer.log_storage_usage()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, customer
AS $$
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
$$;


-- Drop the existing update trigger
DROP TRIGGER IF EXISTS log_storage_usage_trigger ON customer.layer;

-- Insert and delete always move the total.
CREATE OR REPLACE TRIGGER log_storage_usage_trigger
AFTER INSERT OR DELETE
ON customer.layer
FOR EACH ROW
EXECUTE FUNCTION customer.log_storage_usage();

-- An update only does when `size` actually changed. Most updates touch
-- something else entirely (a thumbnail URL, a name, a style), and firing on
-- those wrote `used_storage = used_storage + 0` to the same organization row
-- on every one of them.
CREATE OR REPLACE TRIGGER log_storage_usage_update_trigger
AFTER UPDATE OF size
ON customer.layer
FOR EACH ROW
WHEN (OLD.size IS DISTINCT FROM NEW.size)
EXECUTE FUNCTION customer.log_storage_usage();
