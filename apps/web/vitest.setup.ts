import "@testing-library/jest-dom";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

// Cleanup after each test
afterEach(() => {
  cleanup();
});

// jsdom's CSS parser does not know `@container`, so every render of a
// component that carries a container query raises "Could not parse CSS
// stylesheet" through the virtual console. The rules are valid and are what
// ships; only parsing them here is unsupported, so that one message is
// dropped and every other error still prints.
const printError = console.error;
console.error = (...args: unknown[]) => {
  const first = args[0];
  const message = first instanceof Error ? first.message : String(first ?? "");
  if (message.includes("Could not parse CSS stylesheet")) return;
  printError(...args);
};

// Mock Next.js router
vi.mock("next/navigation", () => ({
  useRouter: () => ({
    push: vi.fn(),
    replace: vi.fn(),
    prefetch: vi.fn(),
    back: vi.fn(),
    pathname: "/",
    query: {},
  }),
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => "/",
}));

// Mock environment variables
process.env.NEXT_PUBLIC_APP_URL = "http://localhost:3000";
// lib/api/* build their base URLs at import time, so anything importing them
// needs these present before the module graph is evaluated.
process.env.NEXT_PUBLIC_API_URL = "http://localhost:8000";
process.env.NEXT_PUBLIC_GEOAPI_URL = "http://localhost:8100";
process.env.NEXT_PUBLIC_PROCESSES_URL = "http://localhost:8300";
