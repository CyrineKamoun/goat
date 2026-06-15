"""Add GIN indexes on record_jsonb for OGC Records filtering

Revision ID: e3f4a5b6c7d8
Revises: d2e3f4a5b6c7
Branch Labels: None
Depends On: None
Create Date: 2026-04-09 00:00:00.000000
"""

from alembic import op

revision = "e3f4a5b6c7d8"
down_revision = "d2e3f4a5b6c7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # GIN index for themes (data category) containment queries:
    #   record_jsonb->'properties'->'themes' @> '[{"concepts":[{"id":"transportation"}]}]'
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_layer_catalog_themes
        ON customer.layer
        USING gin((record_jsonb->'properties'->'themes'))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    # GIN index for full-text search on title + description:
    #   to_tsvector('simple', title || ' ' || description) @@ plainto_tsquery(...)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_layer_catalog_fts
        ON customer.layer
        USING gin(
            to_tsvector('simple',
                coalesce(record_jsonb->'properties'->>'title', '') || ' ' ||
                coalesce(record_jsonb->'properties'->>'description', '')
            )
        )
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    # B-tree index for language and source_format exact-match:
    #   record_jsonb->'properties'->>'language' = 'de'
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_layer_catalog_language
        ON customer.layer ((record_jsonb->'properties'->>'language'))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    # B-tree index for data reference year range queries
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_layer_catalog_year
        ON customer.layer
            (((record_jsonb->'properties'->'extent'->'temporal'->'interval'->0->0)::int))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)

    # B-tree for source_format (iso19139 / dcat / synthetic)
    op.execute("""
        CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_layer_catalog_source_format
        ON customer.layer ((record_jsonb->'properties'->>'source_format'))
        WHERE in_catalog = TRUE AND record_jsonb IS NOT NULL
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_themes")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_fts")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_language")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_year")
    op.execute("DROP INDEX IF EXISTS customer.idx_layer_catalog_source_format")
