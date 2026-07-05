import sharp from "sharp";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";

// The route builds the project URL via `new URL("api/v2/project", API_BASE_URL)`,
// which throws if API_BASE_URL is undefined. `API_BASE_URL` is read from
// `process.env.NEXT_PUBLIC_API_URL` at module-evaluation time in lib/constants,
// so the env var must be set before the route (and its imports) load — hence
// `vi.hoisted`, which vitest hoists above the imports below. The previous
// value is captured and restored in `afterAll` to avoid leaking this
// mutation into other test files sharing the worker process.
const PREV_API_URL = vi.hoisted(() => {
  const prev = process.env.NEXT_PUBLIC_API_URL;
  process.env.NEXT_PUBLIC_API_URL = "http://localhost:8000";
  return prev;
});

import { GET } from "@/app/api/pwa-icon/[projectId]/route";

const PROJECT_ID = "11111111-1111-1111-1111-111111111111";
const SVG = `<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100"><rect width="100" height="100" fill="#2BB381"/></svg>`;

function mockFetch(map: Record<string, () => Response>) {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    for (const [match, make] of Object.entries(map)) {
      if (url.includes(match)) return make();
    }
    return new Response("not found", { status: 404 });
  }));
}

function projectResponse(settings: Record<string, unknown>) {
  return new Response(
    JSON.stringify({
      config: { project: { id: PROJECT_ID, name: "Test Dashboard", builder_config: { settings } } },
    }),
    { status: 200, headers: { "Content-Type": "application/json" } }
  );
}

const req = (size: string) =>
  new Request(`http://localhost:3000/api/pwa-icon/${PROJECT_ID}?size=${size}`);
const params = { params: { projectId: PROJECT_ID } };

afterEach(() => {
  vi.unstubAllGlobals();
});

afterAll(() => {
  if (PREV_API_URL === undefined) delete process.env.NEXT_PUBLIC_API_URL;
  else process.env.NEXT_PUBLIC_API_URL = PREV_API_URL;
});

describe("GET /api/pwa-icon/[projectId]", () => {
  it("returns a PNG of the requested size from the configured app icon", async () => {
    mockFetch({
      "/public": () => projectResponse({ app_icon_url: "https://assets.test/icon.svg" }),
      "assets.test/icon.svg": () =>
        new Response(SVG, { status: 200, headers: { "Content-Type": "image/svg+xml" } }),
    });
    const res = await GET(req("192"), params);
    expect(res.status).toBe(200);
    expect(res.headers.get("Content-Type")).toBe("image/png");
    expect(res.headers.get("Cache-Control")).toBe(
      "public, s-maxage=86400, stale-while-revalidate=604800"
    );
    const meta = await sharp(Buffer.from(await res.arrayBuffer())).metadata();
    expect(meta.width).toBe(192);
  });

  it("falls back to the GOAT logo when no app icon is configured", async () => {
    mockFetch({ "/public": () => projectResponse({}) });
    const res = await GET(req("512"), params);
    expect(res.status).toBe(200);
    const meta = await sharp(Buffer.from(await res.arrayBuffer())).metadata();
    expect(meta.width).toBe(512);
  });

  it("falls back to the GOAT logo when the configured icon is broken", async () => {
    mockFetch({
      "/public": () => projectResponse({ app_icon_url: "https://assets.test/broken.bin" }),
      "assets.test/broken.bin": () => new Response("garbage", { status: 200 }),
    });
    const res = await GET(req("180"), params);
    expect(res.status).toBe(200);
    const meta = await sharp(Buffer.from(await res.arrayBuffer())).metadata();
    expect(meta.width).toBe(180);
  });

  it("rejects disallowed sizes with 400", async () => {
    mockFetch({ "/public": () => projectResponse({}) });
    const res = await GET(req("999"), params);
    expect(res.status).toBe(400);
  });

  it("returns 404 for an unknown project", async () => {
    mockFetch({});
    const res = await GET(req("192"), params);
    expect(res.status).toBe(404);
  });

  it("falls back to the GOAT logo when core is unreachable", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("ECONNREFUSED");
      })
    );
    const res = await GET(req("192"), params);
    expect(res.status).toBe(200);
    const meta = await sharp(Buffer.from(await res.arrayBuffer())).metadata();
    expect(meta.width).toBe(192);
  });

  it("falls back to the GOAT logo when the icon path attempts traversal", async () => {
    mockFetch({
      "/public": () => projectResponse({ app_icon_url: "/../../../etc/passwd" }),
    });
    const res = await GET(req("192"), params);
    expect(res.status).toBe(200);
    const meta = await sharp(Buffer.from(await res.arrayBuffer())).metadata();
    expect(meta.width).toBe(192);
  });
});
