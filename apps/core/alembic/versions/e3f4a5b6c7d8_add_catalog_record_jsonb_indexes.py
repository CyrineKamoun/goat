"""Add GIN indexes on record_jsonb for OGC Records filtering

Revision ID: e3f4a5b6c7d8
Revises: init (previous chain was pruned; DBs are stamped past this revision)
Branch Labels: None
Depends On: None
Create Date: 2026-04-09 00:00:00.000000
"""

from alembic import op

revision = "e3f4a5b6c7d8"
down_revision = "init"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # GIN index for themes (data category) containment queries:
    #   record_jsonb->'properties'->'themes' @> '[{"concepts":[{"id":"transportation"}]}]'
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_layer_catalog_themes
        ON customer.layer
        USING gin((record_jsonb->'properties'->'themes'))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    # GIN index for full-text search on title + description:
    #   to_tsvector('simple', title || ' ' || description) @@ plainto_tsquery(...)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_layer_catalog_fts
        ON customer.layer
        USING gin(
            to_tsvector('simple',
                coalesce(record_jsonb->'properties'->>'title', '') || ' ' ||
                coalesce(record_jsonb->'properties'->>'description', '')
            )
        )
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    # B-tree for language exact-match. Language is an object {"code": "de"};
    # the query filters properties->'language'->>'code', so index that path.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_layer_catalog_language
        ON customer.layer ((record_jsonb->'properties'->'language'->>'code'))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    # B-tree for data reference year. The year lives in the TOP-LEVEL time member
    # (time.interval[0][0] = "YYYY-..."); the query casts substring(...,1,4)::int,
    # so the index expression must be identical to be used.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_layer_catalog_year
        ON customer.layer
            ((substring(record_jsonb->'time'->'interval'->0->>0 from 1 for 4)::int))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    # B-tree for source/provenance format. Filtering is on the flat column
    # upload_file_type (e.g. 'harvesting_opencatalog'), not a record_jsonb field.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_layer_catalog_source_format
        ON customer.layer (upload_file_type)
        WHERE in_catalog = TRUE
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_themes")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_fts")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_language")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_year")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_source_format")
