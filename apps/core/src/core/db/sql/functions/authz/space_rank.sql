CREATE OR REPLACE FUNCTION customer.space_rank(space_id_input UUID, user_id_input UUID)
RETURNS INT
LANGUAGE plpgsql STABLE AS $$
DECLARE
    s RECORD;
BEGIN
    /* 3 = space owner/admin, 2 = member of an editor-default space, 1 = member of a viewer-default space, 0 = not a member */
    SELECT kind, user_id, team_id, organization_id, default_role INTO s FROM customer.space WHERE id = space_id_input;
    IF NOT FOUND OR user_id_input IS NULL THEN
        RETURN 0;
    END IF;
    IF s.kind = 'personal' THEN
        RETURN CASE WHEN s.user_id = user_id_input THEN 3 ELSE 0 END;
    ELSIF s.kind = 'team' THEN
        IF EXISTS (SELECT 1 FROM customer.user_team ut JOIN customer.role r ON r.id = ut.role_id
                   WHERE ut.team_id = s.team_id AND ut.user_id = user_id_input AND r.name = 'team-owner') THEN
            RETURN 3;
        END IF;
        IF EXISTS (SELECT 1 FROM customer.user_team ut WHERE ut.team_id = s.team_id AND ut.user_id = user_id_input) THEN
            RETURN customer.role_rank(s.default_role);
        END IF;
        RETURN 0;
    ELSE
        IF EXISTS (SELECT 1 FROM customer."user" u JOIN customer.user_role ur ON ur.user_id = u.id JOIN customer.role r ON r.id = ur.role_id
                   WHERE u.id = user_id_input AND u.organization_id = s.organization_id
                     AND r.name IN ('organization-owner', 'organization-admin')) THEN
            RETURN 3;
        END IF;
        IF EXISTS (SELECT 1 FROM customer."user" u WHERE u.id = user_id_input AND u.organization_id = s.organization_id) THEN
            RETURN customer.role_rank(s.default_role);
        END IF;
        RETURN 0;
    END IF;
END;
$$;

/* The users holding space_rank = 3 on a space — its owner(s)/admin(s):
   personal → the space's own user; team → members whose user_team role is
   team-owner; organization → users of that organisation holding
   organization-owner or organization-admin. Mirrors space_rank's own rank-3
   conditions, as a set rather than a per-user check. */
CREATE OR REPLACE FUNCTION customer.space_admins(space_id_input UUID)
RETURNS SETOF UUID
LANGUAGE sql STABLE AS $$
    SELECT s.user_id
      FROM customer.space s
     WHERE s.id = space_id_input AND s.kind = 'personal' AND s.user_id IS NOT NULL
    UNION
    SELECT ut.user_id
      FROM customer.space s
      JOIN customer.user_team ut ON ut.team_id = s.team_id
      JOIN customer.role r ON r.id = ut.role_id
     WHERE s.id = space_id_input AND s.kind = 'team' AND r.name = 'team-owner'
    UNION
    SELECT u.id
      FROM customer.space s
      JOIN customer."user" u ON u.organization_id = s.organization_id
      JOIN customer.user_role ur ON ur.user_id = u.id
      JOIN customer.role r ON r.id = ur.role_id
     WHERE s.id = space_id_input AND s.kind = 'organization' AND r.name IN ('organization-owner', 'organization-admin')
$$;
