CREATE OR REPLACE FUNCTION customer.check_resource(
    requested_resource TEXT,
    requested_path TEXT,
    requested_method TEXT
)
RETURNS RECORD AS $$
DECLARE
    rec_resource RECORD;
    extracted_vars JSONB := '{}'::jsonb;
    placeholder_names TEXT[];
    regex_pattern TEXT;
    matches TEXT[];
    i INT;
    path_without_query TEXT;
BEGIN
    
	-- Extract the path from the requested_path by removing query parameters
    path_without_query := COALESCE(SUBSTRING(requested_resource FROM '^[^?]+'), requested_resource);
	
	SELECT *
	INTO rec_resource
	FROM customer.resource
	WHERE path_without_query = url_pattern
    AND requested_method = ANY(method);
	
    IF NOT FOUND THEN
        /* Check if resource matches url-pattern with wildcard */
        SELECT r.*
        INTO rec_resource
        FROM customer.resource r
        WHERE requested_resource LIKE r.url_pattern || '/%'
        AND requested_method = ANY(r.method);
    END IF;

    /* Check if resource exists */
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Resource not found';
    END IF;

    RETURN rec_resource;
END;
$$ LANGUAGE plpgsql;
