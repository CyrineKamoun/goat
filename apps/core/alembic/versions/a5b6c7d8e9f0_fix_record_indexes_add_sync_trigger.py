"""Fix record_jsonb index expressions and persist the record-operational sync trigger

Revision ID: a5b6c7d8e9f0
Revises: f4a5b6c7d8e9
Create Date: 2026-07-22 00:00:00.000000
"""

from alembic import op

revision = "a5b6c7d8e9f0"
down_revision = "f4a5b6c7d8e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Rebuild the three indexes whose expressions did not match the query paths
    # (language is an object {code}, year lives in top-level time.interval, and
    # source_format filtering uses the flat upload_file_type column). DBs stamped
    # past e3f4a5b6c7d8 still carry the old expressions; fresh DBs built the
    # corrected ones there already — IF EXISTS/IF NOT EXISTS keeps both happy.
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_language")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_layer_catalog_language
        ON customer.layer ((record_jsonb->'properties'->'language'->>'code'))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_year")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_layer_catalog_year
        ON customer.layer
            ((substring(record_jsonb->'time'->'interval'->0->>0 from 1 for 4)::int))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_source_format")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_layer_catalog_source_format
        ON customer.layer (upload_file_type)
        WHERE in_catalog = TRUE
    """)

    # Write-time sync of operational fields into record_jsonb, so the stored
    # record stays fresh for raw readers; geoapi's serve-time projection remains
    # the authority for served records (and covers layer_group union extents).
    op.execute("""
        CREATE OR REPLACE FUNCTION customer.sync_record_operational()
        RETURNS trigger LANGUAGE plpgsql AS $$
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
        $$
    """)
    op.execute(
        "DROP TRIGGER IF EXISTS sync_record_operational_trigger ON customer.layer"
    )
    op.execute("""
        CREATE TRIGGER sync_record_operational_trigger
        BEFORE INSERT OR UPDATE OF extent, feature_layer_geometry_type, type, record_jsonb
        ON customer.layer
        FOR EACH ROW EXECUTE FUNCTION customer.sync_record_operational()
    """)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS sync_record_operational_trigger ON customer.layer"
    )
    op.execute("DROP FUNCTION IF EXISTS customer.sync_record_operational()")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_language")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_year")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_source_format")
