CREATE OR REPLACE FUNCTION customer.layer_write_allowed(layer_id_input UUID, user_id_input UUID)
RETURNS BOOLEAN
LANGUAGE plpgsql STABLE AS $$
/* One write rule for a layer: space owner/admin, OR editor-or-owner via
   effective_role('layer', ...) (direct/folder/bundle grants), OR the
   shared-workspace rule — a project containing the layer where the
   requester holds editor-or-owner rank on that project AND the layer's
   own space also does, directly or through one of that space's admins.
   Project access alone never grants write. A layer with no space (a
   genuine orphan — never claimed into one, or one whose space was
   deleted out from under it) is writable by nobody; a catalog-flagged
   layer that does have a space (in_catalog TRUE or catalog_external_uid
   set) is writable by its space owner only — no grant path, including
   the shared-workspace one, may raise it above viewer for anyone else.
   Losing user_id alone (e.g. the creator's account was removed) does not
   lock the layer: the space is still the source of truth for ownership.
   Restricted (D9) narrows write as well as read: a restricted layer — or one
   under a restricted folder or bundle — is not writable through the
   shared-workspace rule, only through the space-admin and grant paths
   above, which `effective_role` already resolves. A non-shareable project
   link (D7) is likewise not a write path: the layer was added by someone who
   could not share it, so the link must not raise anyone's access to it. */
DECLARE
    layer_space UUID;
    is_catalog  BOOLEAN;
BEGIN
    SELECT l.space_id INTO layer_space FROM customer.layer l WHERE l.id = layer_id_input;
    IF NOT FOUND OR layer_space IS NULL THEN
        RETURN FALSE;               -- unknown, or a spaceless (orphan) layer: never writable
    END IF;
    IF customer.role_rank(customer.effective_role('layer', layer_id_input, user_id_input)) = 3 THEN
        RETURN TRUE;
    END IF;
    is_catalog := customer.layer_is_catalog(layer_id_input);
    IF is_catalog THEN
        RETURN FALSE;               -- catalog layers are writable by their space owner only
    END IF;
    /* direct / folder / bundle grants at editor level */
    IF customer.role_rank(customer.effective_role('layer', layer_id_input, user_id_input)) >= 2 THEN
        RETURN TRUE;
    END IF;
    /* shared workspace: a project containing the layer that the requester may edit,
       where the layer's own space — its actual owner — may also edit that project,
       either directly (the project sits in the same space) or through one of that
       space's admins. Anchoring on the space rather than the layer's frozen
       "created by" user_id keeps this from gating on an identity that may no
       longer have anything to do with the layer. The requester-side half is the
       escalation boundary. */
    RETURN NOT customer.restricted_applies(
               'layer', layer_id_input,
               (SELECT l.folder_id FROM customer.layer l WHERE l.id = layer_id_input))
       AND EXISTS (
        SELECT 1
          FROM customer.layer_project lp
          JOIN customer.project p ON p.id = lp.project_id
         WHERE lp.layer_id = layer_id_input
           AND lp.shareable
           AND customer.role_rank(customer.effective_role('project', p.id, user_id_input)) >= 2
           AND (
                 p.space_id IS NOT DISTINCT FROM layer_space
              OR EXISTS (
                     SELECT 1 FROM customer.space_admins(layer_space) a
                      WHERE customer.role_rank(customer.effective_role('project', p.id, a)) >= 2
                 )
           )
    );
END;
$$;
