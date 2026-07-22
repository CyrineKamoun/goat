CREATE OR REPLACE FUNCTION customer.sync_record_operational()
RETURNS TRIGGER AS $$
BEGIN
  -- Operational fields live in flat columns (source of truth for tiles/tools).
  -- When present, sync a one-way copy into record_jsonb so the record is a
  -- self-sufficient OGC document and the read side never has to derive them.
  IF NEW.record_jsonb IS NULL THEN
    RETURN NEW;
  END IF;

  IF NEW.extent IS NOT NULL THEN
    NEW.record_jsonb := jsonb_set(
      NEW.record_jsonb, '{geometry}',
      ST_AsGeoJSON(ST_Envelope(NEW.extent::geometry))::jsonb, true);
  END IF;

  NEW.record_jsonb := jsonb_set(
    NEW.record_jsonb, '{properties,goat:layerType}',
    to_jsonb(NEW.type), true);

  NEW.record_jsonb := jsonb_set(
    NEW.record_jsonb, '{properties,goat:geometryType}',
    to_jsonb(NEW.feature_layer_geometry_type), true);

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER sync_record_operational_trigger
BEFORE INSERT OR UPDATE OF extent, feature_layer_geometry_type, type, record_jsonb
ON customer.layer
FOR EACH ROW
EXECUTE FUNCTION customer.sync_record_operational();
