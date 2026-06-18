from sqlmodel import (
    Column,
    Field,
    Float,
    Text,
)

from core.core.config import settings
from core.db.models._base_class import UUIDServerDefaultBase


class Cost(UUIDServerDefaultBase, table=True):
    """The calculation cost (in credits) per action."""

    __tablename__ = "cost"
    __table_args__ = {"schema": settings.ACCOUNTS_SCHEMA}

    action: str = Field(sa_column=Column(Text, nullable=False), max_length=255)
    credit: float = Field(sa_column=Column(Float, nullable=False))
