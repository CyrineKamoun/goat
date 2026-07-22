CREATE OR REPLACE FUNCTION customer.recompute_group_geometry(gid uuid)
RETURNS void AS $$
BEGIN
  -- A group's footprint is the union of its member layers' extents. Stored in
  -- the group record so the served record is self-sufficient (no serve-time union).
  UPDATE customer.layer_group lg
  SET record_jsonb = jsonb_set(
        lg.record_jsonb, '{geometry}',
        COALESCE((
          SELECT ST_AsGeoJSON(ST_Extent(l.extent::geometry)::geometry)::jsonb
          FROM customer.layer l
          WHERE l.layer_group_id = gid AND l.in_catalog = TRUE AND l.extent IS NOT NULL
        ), 'null'::jsonb), true)
  WHERE lg.id = gid AND lg.record_jsonb IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION customer.sync_group_geometry()
RETURNS TRIGGER AS $$
BEGIN
  -- Recompute the affected group(s) when a member's extent or membership changes;
  -- both old and new groups when a layer moves between them.
  IF TG_OP <> 'INSERT' AND OLD.layer_group_id IS NOT NULL THEN
    PERFORM customer.recompute_group_geometry(OLD.layer_group_id);
  END IF;
  IF TG_OP <> 'DELETE' AND NEW.layer_group_id IS NOT NULL
     AND (TG_OP = 'INSERT' OR NEW.layer_group_id IS DISTINCT FROM OLD.layer_group_id) THEN
    PERFORM customer.recompute_group_geometry(NEW.layer_group_id);
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER sync_group_geometry_trigger
AFTER INSERT OR DELETE OR UPDATE OF extent, layer_group_id
ON customer.layer
FOR EACH ROW
EXECUTE FUNCTION customer.sync_group_geometry();
