CREATE OR REPLACE FUNCTION accounts.extract_params_request(
    requested_resource TEXT,
    requested_path TEXT
)
RETURNS JSONB AS $$
DECLARE
    extracted_vars JSONB := '{}'::jsonb;
    placeholder_names TEXT[];
    regex_pattern TEXT;
    matches TEXT[];
    i INT;
    query_params TEXT := ''; 
    param_pairs TEXT[];
    param_pair TEXT;
    param_key TEXT;
    param_value TEXT;
    param_values TEXT[];
    resource_path TEXT; 
BEGIN
    -- Step 1: Split path and query parameters
    IF position('?' IN requested_path) > 0 THEN
        -- Extract the path without query params
        query_params := split_part(requested_path, '?', 2);
        requested_path := split_part(requested_path, '?', 1);
    END IF;

    IF position('?' IN requested_resource) > 0 THEN
        resource_path := split_part(requested_resource, '?', 1);
    ELSE 
        resource_path := requested_resource;
    END IF;

    -- Step 2: Extract placeholder names from both the path and query strings
    SELECT ARRAY_AGG(u)
    INTO placeholder_names
    FROM regexp_matches(resource_path, '\{([a-zA-Z0-9_]+)\}', 'g') r,
    LATERAL UNNEST(r) u;

    IF placeholder_names IS NOT NULL THEN 
        -- Step 3: Convert the path part of requested_resource into a regex pattern
        regex_pattern := regexp_replace(split_part(resource_path, '?', 1), '\{[a-zA-Z0-9_]+\}', '([a-zA-Z0-9_-]+)', 'g');

        -- Step 4: Extract the actual values from requested_path using the regex pattern
        matches := regexp_matches(requested_path, regex_pattern);

        -- Step 5: Check if any placeholder is missing a corresponding value
        IF array_length(matches, 1) IS DISTINCT FROM array_length(placeholder_names, 1) THEN
            RAISE EXCEPTION 'Path does not contain a corresponding value for each placeholder';
        END IF;

        -- Step 6: Build JSON object with extracted variables from the path
        IF array_length(matches, 1) = array_length(placeholder_names, 1) THEN
            FOR i IN 1..array_length(placeholder_names, 1) LOOP
                -- Check if a placeholder is passed as a value in the path and raise an error if found
                IF matches[i] LIKE '{%' THEN
                    RAISE EXCEPTION 'Placeholder passed as value: %', matches[i];
                END IF;
                extracted_vars := extracted_vars || jsonb_build_object(placeholder_names[i], ARRAY[matches[i]]);
            END LOOP;
        END IF;
    END IF;

    -- Step 7: Handle query parameters placeholders
    IF query_params != '' THEN
        param_pairs := regexp_split_to_array(query_params, '&');
        FOREACH param_pair IN ARRAY param_pairs LOOP
            param_key := split_part(param_pair, '=', 1);
            param_value := split_part(param_pair, '=', 2);

            -- Check if a placeholder is passed as a value in the query params and raise an error if found
            IF param_value LIKE '{%' THEN
                RAISE EXCEPTION 'Placeholder passed as value in query param: %', param_value;
            END IF;

            -- Convert to array if contains ',' or check if the key already exists
            IF position(',' IN param_value) > 0 THEN
                SELECT string_to_array(param_value, ',')
                INTO param_values;
            ELSE
                param_values := ARRAY[param_value];
            END IF;

            -- If the key already exists in extracted_vars, append the values
            IF extracted_vars ? param_key THEN
                extracted_vars := jsonb_set(
                    extracted_vars,
                    ARRAY[param_key],
                    (SELECT jsonb_agg(DISTINCT value::text)
                     FROM (
                        SELECT value::text FROM jsonb_array_elements_text(extracted_vars->param_key)
                        UNION
                        SELECT unnest(param_values)
                     ) AS combined_values
                    )
                );
            ELSE
                -- Add query parameters to the JSON object
                extracted_vars := extracted_vars || jsonb_build_object(param_key, param_values);
            END IF;
        END LOOP;
    END IF;

    RETURN extracted_vars;
END;
$$ LANGUAGE plpgsql;
