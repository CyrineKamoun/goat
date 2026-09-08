from datetime import datetime
from typing import Literal, Optional, Self, Tuple

from pydantic import BaseModel, Field


class DatasetMetadata(BaseModel):
    """
    Canonical metadata describing any dataset ingested or produced by goatlib.

    It is deliberately lightweight — enough for discovery, provenance, and
    OGC API – Features/Processes integration, while keeping I/O modules fast.
    """

    # ---- Identification -------------------------------------------------
    path: str = Field(..., description="Original source path or URI (local/S3/HTTP)")
    source_type: Literal["vector", "tabular", "raster", "remote"] = Field(
        ..., description="High‑level dataset type"
    )

    # ---- Format + driver info -------------------------------------------
    driver: Optional[str] = Field(
        None, description="GDAL, DuckDB or other driver name used to read the data"
    )
    format: Optional[str] = Field(
        None, description="Format/extension after normalisation"
    )

    # ---- Spatial details -------------------------------------------------
    crs: Optional[str] = Field(
        None, description="Coordinate reference system (WKT / EPSG code)"
    )
    geometry_type: Optional[str] = Field(
        None, description="Geometry type for vector layers (Point/Line/Polygon)"
    )
    feature_count: Optional[int] = Field(None, description="Number of features or rows")
    band_count: Optional[int] = Field(None, description="Number of raster bands")
    size: Optional[Tuple[int, int]] = Field(
        None, description="Raster width × height in pixels"
    )

    # ---- Storage + provenance -------------------------------------------
    storage_backend: Optional[str] = Field(
        None, description="Backend used (local | s3 | http)"
    )
    checksum: Optional[str] = Field(
        None, description="Optional SHA‑256 hash of source content"
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when metadata was created",
    )

    # ---- Convenience ----------------------------------------------------
    def short_summary(self: Self) -> str:
        """Return a compact human‑readable summary."""
        info = f"[{self.source_type}] {self.format or ''}"
        extra = []
        if self.feature_count:
            extra.append(f"{self.feature_count} features")
        if self.band_count:
            extra.append(f"{self.band_count} bands")
        if self.size:
            extra.append(f"{self.size[0]}×{self.size[1]} px")
        if extra:
            info += "  – " + ", ".join(extra)
        return info


class ConversionFailure(BaseModel):
    """One dataset that could not be converted, and why."""

    source: str = Field(
        ...,
        description="Discovered path that failed, including `::layer` if it had one.",
    )
    name: str = Field(..., description="What the dataset would have been called.")
    reason: str = Field(..., description="Error message, for reporting to the user.")


class ConvertedDataset(BaseModel):
    """One dataset that converted, and where it came from.

    The source travels with the output because the converted file's name cannot carry it:
    `city.gpkg::roads` and `city_roads.gpkg` both land as `city_roads.parquet`.
    """

    source: str = Field(
        ...,
        description="Discovered path it came from, including `::layer` if it had one.",
    )
    name: str = Field(
        ..., description="Layer name: its own layer name, else the file's stem."
    )
    path: str = Field(..., description="Converted parquet (or tif, for rasters).")
    metadata: DatasetMetadata


class ConversionReport(BaseModel):
    """What came of converting one source: the datasets that made it, and those that did not.

    Both halves matter. An upload can hold several datasets — the layers of a GeoPackage,
    the files of an archive — and one of them being unreadable is no reason to discard the
    rest, nor to leave the person who uploaded it guessing which one was dropped.
    """

    outputs: list[ConvertedDataset] = Field(default_factory=list)
    failures: list[ConversionFailure] = Field(default_factory=list)
