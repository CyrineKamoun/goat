CREATE OR REPLACE FUNCTION customer.log_user()
RETURNS TRIGGER AS $$
DECLARE
    organization_id_input UUID;
BEGIN

    -- Get the organization_id.
    IF TG_TABLE_NAME IN ('user', 'invitation') THEN
        IF NEW.organization_id IS NOT NULL THEN
            organization_id_input := NEW.organization_id;
        ELSE
            organization_id_input := OLD.organization_id;
        END IF;
    ELSIF TG_TABLE_NAME = 'user_role' THEN
        SELECT organization_id
        INTO organization_id_input
        FROM customer.user u
        JOIN customer.user_role r ON u.id = r.user_id
        WHERE r.user_id = COALESCE(NEW.user_id, OLD.user_id);
    END IF;

    WITH user_roles AS (
        SELECT r.*
        FROM customer.user u
        JOIN customer.user_role r ON u.id = r.user_id
        WHERE u.organization_id = organization_id_input
    ),
    role_counts AS (
        SELECT 
            COUNT(CASE WHEN r.name LIKE '%viewer' THEN 1 END) AS viewer_count,
            COUNT(CASE WHEN r.name NOT LIKE '%viewer' THEN 1 END) AS editor_count
        FROM 
        (
            SELECT name
            FROM customer.role r
            JOIN user_roles ur ON r.id = ur.role_id
            WHERE r.resource_type = 'organization'
            UNION ALL 
            SELECT (payload ->> 'role')::text
            FROM customer.invitation
            WHERE organization_id = organization_id_input
            AND TYPE = 'organization'
            AND status = 'pending'
        ) r
    )
    UPDATE customer.organization o
    SET used_viewers = rc.viewer_count,
    used_editors = rc.editor_count
    FROM role_counts rc
    WHERE o.id = organization_id_input;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a single trigger for INSERT, UPDATE, and DELETE
CREATE OR REPLACE TRIGGER log_user_trigger
AFTER INSERT OR UPDATE OR DELETE
ON customer.user
FOR EACH ROW
EXECUTE FUNCTION customer.log_user();

-- Create a single trigger for INSERT, UPDATE, and DELETE
CREATE OR REPLACE TRIGGER log_user_role_trigger
AFTER INSERT OR UPDATE OR DELETE
ON customer.user_role
FOR EACH ROW
EXECUTE FUNCTION customer.log_user();

-- Trigger for invitation if of type organization
CREATE OR REPLACE TRIGGER log_invitation_trigger
AFTER INSERT OR UPDATE OR DELETE
ON customer.invitation
FOR EACH ROW
EXECUTE FUNCTION customer.log_user();

