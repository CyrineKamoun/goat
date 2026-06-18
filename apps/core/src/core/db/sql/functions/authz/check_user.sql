CREATE OR REPLACE FUNCTION customer.check_user(
    user_id UUID,
    rec_resource RECORD
)
RETURNS RECORD AS $$
DECLARE
    rec_user RECORD;
    rec_role RECORD;
BEGIN
    /*Get user into a record*/
    SELECT u.*
    INTO rec_user
    FROM customer.user u
    WHERE u.id = user_id; 

    /*Check if user exists*/
    IF NOT FOUND THEN
        RAISE EXCEPTION 'User not found';
    END IF;

    /*Get role into a record*/
    WITH user_role AS (
        SELECT ur.role_id
        FROM customer.user_role ur
        WHERE ur.user_id = rec_user.id
    )
    SELECT r.* 
    INTO rec_role
    FROM user_role ur, customer.role r 
    WHERE ur.role_id = r.id;

    /*Check if role exists*/
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Role not found';
    END IF; 

    /*Check if each needed permissions is in the user permissions*/
    IF EXISTS (
        WITH needed_permissions AS (
            SELECT p.id
            FROM customer.permission p
            JOIN customer.resource_permission r ON p.id = r.permission_id
            WHERE r.resource_id = rec_resource.id
        ), 
        user_permissions AS (
            SELECT p.id
            FROM customer.permission p
            JOIN customer.role_permission r ON p.id = r.permission_id
            WHERE r.role_id = rec_role.id
        )
        SELECT 1
        FROM needed_permissions np
        WHERE NOT EXISTS (
            SELECT 1
            FROM user_permissions up
            WHERE np.id = up.id
        )
    ) THEN
        RAISE EXCEPTION 'User does not have the needed permissions';
    END IF;

    RETURN rec_user;
END;
$$ LANGUAGE plpgsql;