"""Shared routing-mode budget constants for catchment / heatmap v2 tools.

Centralised so the per-mode time / distance caps stay consistent across
catchment_area_v2 and heatmap_v2 form schemas.
"""

# Defaults (minutes / metres).
DEFAULT_MAX_TIME_ACTIVE_MIN = 15
DEFAULT_MAX_TIME_CAR_MIN = 30
DEFAULT_MAX_TIME_PT_MIN = 30
DEFAULT_MAX_DISTANCE_ACTIVE_M = 500
DEFAULT_MAX_DISTANCE_CAR_M = 5000

# Per-mode hard caps.
MAX_TIME_ACTIVE_MIN = 45
MAX_TIME_CAR_MIN = 90
MAX_TIME_PT_MIN = 90
MAX_DISTANCE_ACTIVE_M = 20000
MAX_DISTANCE_CAR_M = 100000

# i18n message keys (defined in goatlib/i18n/translations/{en,de}.json and
# apps/web/i18n/locales/{en,de}/common.json).
ACTIVE_TIME_LIMIT_MSG = "active_mobility_time_limit_message"
CAR_TIME_LIMIT_MSG = "car_time_limit_message"
PT_TIME_LIMIT_MSG = "pt_time_limit_message"
ACTIVE_DISTANCE_LIMIT_MSG = "active_mobility_distance_limit_message"
CAR_DISTANCE_LIMIT_MSG = "car_distance_limit_message"
