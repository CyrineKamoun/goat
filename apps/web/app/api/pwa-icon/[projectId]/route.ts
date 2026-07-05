import { promises as fs } from "fs";
import path from "path";

import { API_BASE_URL } from "@/lib/constants";
import { isAllowedIconSize, rasterizeToPng, resolveAppIconUrl } from "@/lib/pwa/icon";

export const runtime = "nodejs";

const GOAT_LOGO_PATH = path.join(process.cwd(), "public/assets/svg/goat-logo.svg");
const CACHE_CONTROL = "public, s-maxage=86400, stale-while-revalidate=604800";

async function loadSourceImage(iconUrl: string | null): Promise<Buffer> {
  if (iconUrl) {
    // Relative URLs ("/assets/...") are files in our own public dir;
    // absolute URLs live on the assets store (S3).
    if (iconUrl.startsWith("/")) {
      const publicRoot = path.resolve(process.cwd(), "public");
      const resolved = path.resolve(publicRoot, `.${iconUrl}`);
      if (!resolved.startsWith(publicRoot + path.sep)) {
        throw new Error("icon path escapes public dir");
      }
      return fs.readFile(resolved);
    }
    const res = await fetch(iconUrl);
    if (!res.ok) throw new Error(`icon fetch failed: ${res.status}`);
    return Buffer.from(await res.arrayBuffer());
  }
  return fs.readFile(GOAT_LOGO_PATH);
}

export async function GET(
  request: Request,
  { params }: { params: { projectId: string } }
): Promise<Response> {
  const size = Number(new URL(request.url).searchParams.get("size") ?? "192");
  if (!isAllowedIconSize(size)) {
    return new Response("Invalid size. Allowed: 180, 192, 512.", { status: 400 });
  }

  const projectUrl = `${new URL("api/v2/project", API_BASE_URL).href}/${params.projectId}/public`;
  let settings: { app_icon_url?: string | null } | null | undefined = null;
  try {
    const projectRes = await fetch(projectUrl, { cache: "no-store" });
    if (!projectRes.ok) return new Response("Project not found", { status: 404 });
    const publicProject = await projectRes.json();
    settings = publicProject?.config?.project?.builder_config?.settings;
  } catch {
    // Core unreachable or malformed payload — a broken upstream must not
    // break installability; serve the GOAT logo.
    settings = null;
  }

  // Broken icons must degrade to the GOAT logo, never to an error —
  // a failing icon must not break installability.
  let png: Buffer;
  try {
    png = await rasterizeToPng(await loadSourceImage(resolveAppIconUrl(settings)), size);
  } catch {
    png = await rasterizeToPng(await fs.readFile(GOAT_LOGO_PATH), size);
  }

  return new Response(new Uint8Array(png), {
    status: 200,
    headers: { "Content-Type": "image/png", "Cache-Control": CACHE_CONTROL },
  });
}
