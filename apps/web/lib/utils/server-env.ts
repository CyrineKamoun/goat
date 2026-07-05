import { publicEnv } from "@/lib/utils/public-env";

/**
 * Server-runtime resolution of values whose NEXT_PUBLIC_* twins are only
 * inlined into the client bundle. Server code (API routes, RSC, middleware)
 * reads process.env at request time, so it must prefer the backend var names
 * and fall back to the NEXT_PUBLIC_* names for deployments that only set
 * those (e.g. the prebuilt Docker image).
 */

export const serverAppUrl = (): string | undefined =>
  process.env.NEXTAUTH_URL || publicEnv(process.env.NEXT_PUBLIC_APP_URL);

export const serverKeycloakIssuer = (): string | undefined =>
  process.env.KEYCLOAK_SERVER_URL && process.env.REALM_NAME
    ? `${process.env.KEYCLOAK_SERVER_URL}/realms/${process.env.REALM_NAME}`
    : publicEnv(process.env.NEXT_PUBLIC_KEYCLOAK_ISSUER);

export const serverKeycloakClientId = (): string | undefined =>
  process.env.KEYCLOAK_CLIENT_ID || publicEnv(process.env.NEXT_PUBLIC_KEYCLOAK_CLIENT_ID);
