from enum import Enum


class DataStoreType(str, Enum):
    """Data store type."""

    postgis = "postgis"
    external = "external"
