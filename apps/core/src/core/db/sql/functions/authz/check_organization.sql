CREATE OR REPLACE FUNCTION accounts.check_organization(
    rec_user RECORD,
    rec_resource RECORD,
    organization_id UUID
)
RETURNS RECORD AS $$
DECLARE
    rec_organization RECORD;
    rec_organization_json JSONB;
    total_quota_column TEXT;
    used_quota_column TEXT;
BEGIN
    /*Get organization into a record.*/
   	SELECT o.*
   	INTO rec_organization
   	FROM accounts.organization o
   	WHERE o.id = rec_user.organization_id;

    /*Check if organization exists*/
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Organization not found';
    END IF;

    /*Make sure organization from request params corresponds to organization from user*/
    IF organization_id IS NOT NULL THEN
        IF rec_user.organization_id != organization_id THEN
            RAISE EXCEPTION 'Organization from request params does not correspond to organization from user';
        END IF;
    END IF;
    rec_organization_json := to_json(rec_organization);

    /*Check if role has an active supscription or is suspended*/
    IF rec_organization.suspended IS TRUE THEN
        RAISE EXCEPTION 'Organization is suspended';
    END IF;

    /*Check if request need specific subscription if so check if organization has subscription*/
    IF rec_resource.plan_names IS NOT NULL THEN
        IF rec_organization.plan_name NOT IN (SELECT UNNEST(rec_resource.plan_names)) THEN
            RAISE EXCEPTION 'Organization does not have the required subscription';
        END IF; 
    END IF;

    /*Check if request affects quota*/
    IF rec_resource.quota_types IS NOT NULL THEN
        /*Loop through quota_types and check rec_organization quota. Get name of column like total_ +  quota_type*/
        FOR i IN 1..array_length(rec_resource.quota_types, 1) LOOP
            total_quota_column := 'total_' || rec_resource.quota_types[i];
            used_quota_column := 'used_' || rec_resource.quota_types[i];

            IF (rec_organization_json ->> used_quota_column)::text::float >= (rec_organization_json ->> total_quota_column)::text::float THEN
                RAISE EXCEPTION 'Organization has reached quota for %', rec_resource.quota_types[i];
            END IF;
        END LOOP;
    END IF;

    RETURN rec_organization;
END;
$$ LANGUAGE plpgsql;