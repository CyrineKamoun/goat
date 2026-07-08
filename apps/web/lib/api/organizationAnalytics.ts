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

/** Lists the org's analytics instances (empty array when none configured). */
export function useOrganizationAnalytics(organizationId: string | undefined) {
  const { data, error, isLoading, mutate } = useSWR<OrganizationAnalytics[]>(
    organizationId ? [orgAnalyticsUrl(organizationId)] : null,
    fetcher,
    { revalidateOnFocus: false }
  );
  return { analyticsList: data ?? [], isLoading, isError: error, mutate };
}

export async function createOrganizationAnalytics(
  organizationId: string,
  payload: OrganizationAnalyticsCreate
): Promise<OrganizationAnalytics> {
  const response = await apiRequestAuth(orgAnalyticsUrl(organizationId), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error("Failed to create analytics instance");
  }
  return await response.json();
}

export async function updateOrganizationAnalytics(
  organizationId: string,
  analyticsId: string,
  payload: OrganizationAnalyticsCreate
): Promise<OrganizationAnalytics> {
  const response = await apiRequestAuth(
    `${orgAnalyticsUrl(organizationId)}${analyticsId}`,
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }
  );
  if (!response.ok) {
    throw new Error("Failed to update analytics instance");
  }
  return await response.json();
}

export async function deleteOrganizationAnalytics(
  organizationId: string,
  analyticsId: string
): Promise<void> {
  const response = await apiRequestAuth(
    `${orgAnalyticsUrl(organizationId)}${analyticsId}`,
    { method: "DELETE" }
  );
  if (!response.ok && response.status !== 204) {
    throw new Error("Failed to delete analytics instance");
  }
}

async function updateTrackingSettings(
  projectId: string,
  body: { analytics_id?: string | null; require_consent?: boolean }
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

/** Assign an analytics instance to a published project; null turns tracking off. */
export function setProjectAnalytics(projectId: string, analyticsId: string | null) {
  return updateTrackingSettings(projectId, { analytics_id: analyticsId });
}

export function setProjectTrackingRequireConsent(projectId: string, requireConsent: boolean) {
  return updateTrackingSettings(projectId, { require_consent: requireConsent });
}
