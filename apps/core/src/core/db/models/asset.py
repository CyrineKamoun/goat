from enum import Enum
from uuid import UUID

from sqlalchemy import Index, text
from sqlalchemy.dialects.postgresql import UUID as UUID_PG
from sqlmodel import BigInteger, Column, Field, ForeignKey, String, Text

from core.core.config import settings
from core.db.models._base_class import DateTimeBase


class AssetType(str, Enum):
    IMAGE = "image"
    ICON = "icon"
    DOCUMENT = "document"


class UploadedAsset(DateTimeBase, table=True):
    __tablename__ = "uploaded_asset"
    __table_args__ = {"schema": settings.CUSTOMER_SCHEMA}

    id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            primary_key=True,
            nullable=False,
            server_default=text("uuid_generate_v4()"),
        ),
    )
    user_id: UUID = Field(
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.ACCOUNTS_SCHEMA}.user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        description="ID of the user who uploaded the asset.",
    )
    folder_id: UUID | None = Field(
        default=None,
        sa_column=Column(
            UUID_PG(as_uuid=True),
            ForeignKey(f"{settings.CUSTOMER_SCHEMA}.folder.id", ondelete="SET NULL"),
            nullable=True,
        ),
        description="Optional folder this asset belongs to.",
    )
    s3_key: str = Field(
        sa_column=Column(
            Text,
            nullable=False,
            unique=True,
        ),
        description="Unique key/path of the file in the S3 bucket.",
    )
    file_name: str = Field(
        sa_column=Column(String(255), nullable=False),
        description="The original filename provided by the user.",
    )
    display_name: str | None = Field(
        default=None,
        sa_column=Column(String(255), nullable=True),
        description="Optional display name for the asset.",
    )
    category: str | None = Field(
        default=None,
        sa_column=Column(String(100), nullable=True),
        description="Optional category for organizing assets (e.g., 'markers', 'backgrounds').",
    )
    mime_type: str = Field(
        sa_column=Column(String(100), nullable=False),
        description="MIME type of the uploaded file (e.g., image/jpeg, image/svg+xml).",
    )
    file_size: int = Field(
        default=None,
        sa_column=Column(BigInteger, nullable=False),
        description="Size of the file in bytes.",
    )
    asset_type: AssetType = Field(
        sa_column=Column(
            String(50),
            nullable=False,
        ),
        description="Type of asset: 'image', 'icon', or 'document'.",
    )
    content_hash: str = Field(
        sa_column=Column(
            String(64),
            nullable=False,
        ),
        description="SHA256 hash of the file content for deduplication.",
    )


Index("idx_uploaded_asset_folder_id", UploadedAsset.__table__.c.folder_id)
