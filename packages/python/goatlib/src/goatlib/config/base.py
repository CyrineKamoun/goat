from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseSettingsModel(BaseSettings):
    """Base configuration model with uv/.env support."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class CommonSettings(BaseSettingsModel):
    """Common environment variables shared across services."""

    environment: Literal["dev", "staging", "prod"] = "dev"
    log_level: str = "INFO"


class RoutingSettings(BaseSettingsModel):
    """Routing service configuration."""

    # Shared authorization for all routing services
    routing_authorization: str | None = None

    # GOAT Routing service
    goat_routing_url: str = "http://localhost:8200/api/v2/routing"

    # Local C++ routing backend paths
    street_network_edges_base_path: str = "/app/data/street_network/edges"
    street_network_nodes_base_path: str = "/app/data/street_network/nodes"
    pt_network_base_path: str = "/app/data/pt_network/latest_de.bin"

    # R5 service
    r5_url: str = "http://localhost:7070"
    r5_variant_index: int = -1
    r5_worker_version: str = "v6.4"

    # Request settings
    request_timeout: int = 60
    request_retries: int = 10
    request_retry_interval: float = 2.0
