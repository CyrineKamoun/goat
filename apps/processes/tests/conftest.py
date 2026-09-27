"""Pytest configuration for Processes API tests."""

import os

# processes.config builds its settings at import and refuses AUTH on (the
# default) without a Keycloak server; this placeholder satisfies that check.
os.environ.setdefault("KEYCLOAK_SERVER_URL", "http://keycloak.test")
