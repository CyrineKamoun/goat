import { serverAppUrl } from "@/lib/utils/server-env";

const DEFAULT_THEME_COLOR = "#2BB381";
const SHORT_NAME_MAX = 30;

export type PublicProjectForManifest = {
  id: string;
  name: string;
  builder_config?: { settings?: { primary_color?: string | null } | null } | null;
};

export type WebAppManifest = {
  name: string;
  short_name: string;
  id: string;
  start_url: string;
  scope: string;
  display: "standalone";
  theme_color: string;
  background_color: string;
  icons: Array<{ src: string; sizes: string; type: string; purpose?: string }>;
};

// Mirrors the host comparison in middlewares/withCustomDomain.ts: the
// request host (port stripped) vs. the app URL's hostname.
export function isCustomDomainHost(host: string | null): boolean {
  const appUrl = serverAppUrl();
  if (!host || !appUrl) return false;
  let canonical: string;
  try {
    canonical = new URL(appUrl).hostname.toLowerCase();
  } catch {
    return false;
  }
  return host.split(":")[0].toLowerCase() !== canonical;
}

export function buildManifest(
  project: PublicProjectForManifest,
  opts: { isCustomDomain: boolean }
): WebAppManifest {
  // On custom domains the middleware rewrite hides /map/public/<id>;
  // the app lives at the domain root.
  const base = opts.isCustomDomain ? "/" : `/map/public/${project.id}`;
  const icon = (size: number) => `/api/pwa-icon/${project.id}?size=${size}`;
  return {
    name: project.name,
    short_name: project.name.slice(0, SHORT_NAME_MAX),
    id: project.id,
    start_url: base,
    scope: base,
    display: "standalone",
    theme_color: project.builder_config?.settings?.primary_color || DEFAULT_THEME_COLOR,
    background_color: "#ffffff",
    icons: [
      { src: icon(192), sizes: "192x192", type: "image/png" },
      { src: icon(512), sizes: "512x512", type: "image/png" },
      { src: icon(512), sizes: "512x512", type: "image/png", purpose: "maskable" },
    ],
  };
}
