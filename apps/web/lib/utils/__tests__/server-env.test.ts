import { afterEach, describe, expect, it, vi } from "vitest";

import { serverAppUrl, serverKeycloakClientId, serverKeycloakIssuer } from "@/lib/utils/server-env";

describe("server-env", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("derives the keycloak issuer from server vars", () => {
    vi.stubEnv("KEYCLOAK_SERVER_URL", "https://auth.example.com");
    vi.stubEnv("REALM_NAME", "p4b");
    expect(serverKeycloakIssuer()).toBe("https://auth.example.com/realms/p4b");
  });

  it("falls back to the public issuer when server vars are unset", () => {
    vi.stubEnv("KEYCLOAK_SERVER_URL", "");
    vi.stubEnv("NEXT_PUBLIC_KEYCLOAK_ISSUER", "https://auth.example.com/realms/pub");
    expect(serverKeycloakIssuer()).toBe("https://auth.example.com/realms/pub");
  });

  it("ignores the unsubstituted issuer placeholder", () => {
    vi.stubEnv("KEYCLOAK_SERVER_URL", "");
    vi.stubEnv("NEXT_PUBLIC_KEYCLOAK_ISSUER", "APP_NEXT_PUBLIC_KEYCLOAK_ISSUER");
    expect(serverKeycloakIssuer()).toBeUndefined();
  });

  it("prefers the server client id over the public one", () => {
    vi.stubEnv("KEYCLOAK_CLIENT_ID", "goat-web");
    vi.stubEnv("NEXT_PUBLIC_KEYCLOAK_CLIENT_ID", "other");
    expect(serverKeycloakClientId()).toBe("goat-web");
  });

  it("derives the app url from NEXTAUTH_URL with public fallback", () => {
    vi.stubEnv("NEXTAUTH_URL", "http://localhost:3000");
    expect(serverAppUrl()).toBe("http://localhost:3000");
    vi.stubEnv("NEXTAUTH_URL", "");
    vi.stubEnv("NEXT_PUBLIC_APP_URL", "https://goat.example.com");
    expect(serverAppUrl()).toBe("https://goat.example.com");
  });
});
