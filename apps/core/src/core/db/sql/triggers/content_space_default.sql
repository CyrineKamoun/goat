CREATE OR REPLACE FUNCTION customer.content_space_default()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.space_id IS NULL AND NEW.folder_id IS NOT NULL THEN
        NEW.space_id := (SELECT space_id FROM customer.folder WHERE id = NEW.folder_id);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS content_space_default ON customer.layer;
CREATE TRIGGER content_space_default
BEFORE INSERT ON customer.layer
FOR EACH ROW EXECUTE FUNCTION customer.content_space_default();

DROP TRIGGER IF EXISTS content_space_default ON customer.project;
CREATE TRIGGER content_space_default
BEFORE INSERT ON customer.project
FOR EACH ROW EXECUTE FUNCTION customer.content_space_default();

DROP TRIGGER IF EXISTS content_space_default ON customer.bundle;
CREATE TRIGGER content_space_default
BEFORE INSERT ON customer.bundle
FOR EACH ROW EXECUTE FUNCTION customer.content_space_default();
