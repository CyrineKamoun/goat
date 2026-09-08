/**
 * Fetcher for public, third-party feeds (docs site, status site) that must
 * never see the caller's bearer token — unlike the shared `fetcher` in
 * `@/lib/api/fetcher`, which attaches `Authorization: Bearer <token>` to
 * every request regardless of host.
 */
export const publicFetcher = async <T = unknown>(url: string): Promise<T> => {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  return response.json();
};
