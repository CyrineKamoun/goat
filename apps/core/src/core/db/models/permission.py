from sqlmodel import Column, Field, Text

from core.core.config import settings
from core.db.models._base_class import UUIDServerDefaultBase


class Permission(UUIDServerDefaultBase, table=True):
    """A permission identified by a slug (e.g. ``read-layer``)."""

    __tablename__ = "permission"
    __table_args__ = {"schema": settings.SCHEMA}

    slug: str = Field(sa_column=Column(Text, nullable=False))
