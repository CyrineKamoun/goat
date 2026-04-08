from typing import Self

from goatlib.config.base import CommonSettings, RoutingSettings
from goatlib.config.io import IOSettings
from goatlib.config.print import PrintSettings


class Settings:
    """Unified access point for all config domains."""

    def __init__(self: Self) -> None:
        self.common = CommonSettings()
        self.io = IOSettings()
        self.routing = RoutingSettings()
        self.print = PrintSettings()


settings = Settings()
