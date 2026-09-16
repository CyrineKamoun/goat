import { isAuthDisabled } from "@/lib/utils/auth-flag";
import { publicEnv } from "@/lib/utils/public-env";

export const KEYCLOAK_CLIENT_ID = process.env.NEXT_PUBLIC_KEYCLOAK_CLIENT_ID;
export const KEYCLOAK_ISSUER = process.env.NEXT_PUBLIC_KEYCLOAK_ISSUER;
export const GEOAPI_BASE_URL = process.env.NEXT_PUBLIC_GEOAPI_URL;
export const PROCESSES_BASE_URL = process.env.NEXT_PUBLIC_PROCESSES_URL;
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL;
export const APP_URL = process.env.NEXT_PUBLIC_APP_URL;
/**
 * STAC API of the catalog service (apps/catalog).
 *
 * Falls back to `${GEOAPI_BASE_URL}/stac`, which is where the ingress
 * path-routes it in every deployed environment (design S7) — so only local
 * development, where the service answers directly on :8400, needs the
 * variable set.
 */
export const CATALOG_BASE_URL =
  process.env.NEXT_PUBLIC_CATALOG_URL ?? (GEOAPI_BASE_URL ? `${GEOAPI_BASE_URL}/stac` : undefined);
export const AUTH_DISABLED = isAuthDisabled(process.env.NEXT_PUBLIC_AUTH);

export const DOCS_URL = "https://goat.plan4better.de/docs";
export const CONTACT_US_URL = "https://plan4better.de/contact";
/**
 * The public website. `NEXT_PUBLIC_WEBSITE_URL` names the deployment whose
 * blog and GOAT changelog RSS feeds fill Home's "From our blog" and "What's
 * new"; when it is unset (or still the Docker placeholder) those surfaces
 * stay hidden and only the plain links fall back to production.
 */
const configuredWebsiteUrl = publicEnv(process.env.NEXT_PUBLIC_WEBSITE_URL)?.replace(/\/+$/, "");
export const WEBSITE_URL = configuredWebsiteUrl ?? "https://www.plan4better.de";
export const WEBSITE_FEEDS_ENABLED = Boolean(configuredWebsiteUrl);

/** The catchment-area walkthrough the help strip links to, one cut per UI language. */
export const HELP_VIDEO_URL: Record<"en" | "de", string> = {
  en: "https://www.youtube.com/watch?v=_clsR386b9w",
  de: "https://www.youtube.com/watch?v=GA_6PbhAA6k",
};
/**
 * The product intro the first-run Welcome plays: a Bunny Stream video on the
 * product's video CDN, which generates the poster and the MP4 renditions
 * beside the HLS playlist. The 720p MP4 plays in a `<video>` everywhere.
 */
const WELCOME_VIDEO_BASE = "https://videos.plan4better.de/31ad707d-c1f2-4f21-8c47-1c610141f9d2";
export const WELCOME_VIDEO_URL = `${WELCOME_VIDEO_BASE}/play_720p.mp4`;
export const WELCOME_VIDEO_POSTER = `${WELCOME_VIDEO_BASE}/thumbnail.jpg`;
export const MAPBOX_TOKEN = process.env.NEXT_PUBLIC_MAPBOX_TOKEN ?? "";

export const MAPTILER_KEY = "tffQ1wAu9TKyVMHrc3o3";

export const ORG_DEFAULT_AVATAR = "https://assets.plan4better.de/img/no-org-thumb.jpg";

export const STREET_NETWORK_LAYER_ID = "903ecdca-b717-48db-bbce-0219e41439cf";
export const SYSTEM_LAYERS_IDS = [STREET_NETWORK_LAYER_ID];

export const GEOFENCE_LAYERS_PATH = "https://assets.plan4better.de/other/geofence";
export const DEFAULT_WKT_EXTENT = "POLYGON((-180 -90, -180 90, 180 90, 180 -90, -180 -90))";

export const ASSETS_URL = "https://assets.plan4better.de";

export const MAX_EDITABLE_LAYER_SIZE = 100 * 1024 * 1024; // 100MB

export const THEME_COOKIE_NAME = "client_theme";
export const LANGUAGE_COOKIE_NAME = "client_language";
