CREATE OR REPLACE FUNCTION customer.folder_depth_check()
RETURNS TRIGGER AS $$
DECLARE
    parent_space UUID;
    chain_len    INT;
    height       INT;
BEGIN
    IF NEW.parent_id IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.parent_id = NEW.id THEN
        RAISE EXCEPTION 'folder cannot be its own parent';
    END IF;
    SELECT f.space_id INTO parent_space FROM customer.folder f WHERE f.id = NEW.parent_id;
    IF parent_space IS DISTINCT FROM NEW.space_id THEN
        RAISE EXCEPTION 'parent folder must be in the same space';
    END IF;
    /* length of the parent's own chain (parent + its ancestors, capped at 4 by folder_chain) */
    SELECT count(*) INTO chain_len FROM customer.folder_chain(NEW.parent_id);
    /* height of NEW's own subtree below it — 0 when NEW has no children yet (always true on
       INSERT, since NEW.id cannot already be referenced as a parent_id); a nonzero height on
       UPDATE means NEW is being moved along with its existing descendants, who would land one
       level deeper for every level NEW itself moves down */
    SELECT COALESCE(MAX(depth), 0) INTO height FROM (
        WITH RECURSIVE sub AS (
            SELECT id, 1 AS depth FROM customer.folder WHERE parent_id = NEW.id
            UNION ALL
            SELECT f.id, s.depth + 1
            FROM customer.folder f
            JOIN sub s ON f.parent_id = s.id
            WHERE s.depth < 4
        )
        SELECT depth FROM sub
    ) x;
    IF chain_len + 1 + height > 3 THEN
        RAISE EXCEPTION 'folder depth limit (3) exceeded';
    END IF;
    IF EXISTS (SELECT 1 FROM customer.folder_chain(NEW.parent_id) fc WHERE fc = NEW.id) THEN
        RAISE EXCEPTION 'folder cannot be moved under its own descendant';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS folder_depth_check ON customer.folder;
CREATE TRIGGER folder_depth_check
BEFORE INSERT OR UPDATE OF parent_id, space_id ON customer.folder
FOR EACH ROW EXECUTE FUNCTION customer.folder_depth_check();
