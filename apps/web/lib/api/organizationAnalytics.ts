import useSWR from "swr";

import { apiRequestAuth, fetcher } from "@/lib/api/fetcher";
import type {
  OrganizationAnalytics,
  OrganizationAnalyticsCreate,
} from "@/lib/validations/organizationAnalytics";

const ORGANIZATIONS_API_BASE_URL = new URL(
  "api/v2/organizations",
  process.env.NEXT_PUBLIC_API_URL
).href;

const orgAnalyticsUrl = (orgId: string) =>
  `${ORGANIZATIONS_API_BASE_URL}/${orgId}/analytics/`;

/**
 * Returns the org's analytics config, or `null` when not set yet.
 * Backend returns 200 + null body for unconfigured orgs (the analytics
 * singleton just hasn't been set), so this hook is a plain SWR call.
 */
export function useOrganizationAnalytics(organizationId: string | undefined) {
  const { data, error, isLoading, mutate } = useSWR<OrganizationAnalytics | null>(
    organizationId ? [orgAnalyticsUrl(organizationId)] : null,
    fetcher,
    { revalidateOnFocus: false }
  );
  return { analytics: data ?? null, isLoading, isError: error, mutate };
}

export async function upsertOrganizationAnalytics(
  organizationId: string,
  payload: OrganizationAnalyticsCreate
): Promise<OrganizationAnalytics> {
  const response = await apiRequestAuth(orgAnalyticsUrl(organizationId), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error("Failed to save analytics configuration");
  }
  return await response.json();
}

export async function deleteOrganizationAnalytics(
  organizationId: string
): Promise<void> {
  const response = await apiRequestAuth(orgAnalyticsUrl(organizationId), {
    method: "DELETE",
  });
  if (!response.ok && response.status !== 204) {
    throw new Error("Failed to remove analytics configuration");
  }
}

async function updateTrackingSettings(
  projectId: string,
  body: { enabled?: boolean; require_consent?: boolean }
): Promise<void> {
  const url = new URL(
    `api/v2/project/${projectId}/public/tracking`,
    process.env.NEXT_PUBLIC_API_URL
  ).href;
  const response = await apiRequestAuth(url, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    throw new Error("Failed to update tracking setting");
  }
}

export function setProjectTrackingEnabled(projectId: string, enabled: boolean) {
  return updateTrackingSettings(projectId, { enabled });
}

export function setProjectTrackingRequireConsent(projectId: string, requireConsent: boolean) {
  return updateTrackingSettings(projectId, { require_consent: requireConsent });
}
