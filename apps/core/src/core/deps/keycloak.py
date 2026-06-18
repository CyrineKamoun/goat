from core.core.config import settings
from keycloak import KeycloakAdmin, KeycloakOpenIDConnection


async def keycloak_admin() -> KeycloakAdmin | None:
    """Admin client for the configured realm, authenticated as a service account
    via the client-credentials grant (least-privilege; no master-realm admin).

    Returns ``None`` when the service-account client isn't configured, so callers
    degrade gracefully (skip Keycloak enrichment / writes) instead of crashing.
    """
    if not (settings.KEYCLOAK_CLIENT_ID and settings.KEYCLOAK_CLIENT_SECRET):
        return None
    connection = KeycloakOpenIDConnection(
        server_url=settings.KEYCLOAK_SERVER_URL,
        realm_name=settings.REALM_NAME,
        client_id=settings.KEYCLOAK_CLIENT_ID,
        client_secret_key=settings.KEYCLOAK_CLIENT_SECRET,
        verify=True,
    )
    return KeycloakAdmin(connection=connection)
