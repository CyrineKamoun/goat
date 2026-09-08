import { describe, expect, it, vi } from "vitest";

import { uploadFileToS3 } from "@/lib/services/s3";

const presigned = { url: "http://s3.test/bucket", fields: { key: "k" } };

describe("uploadFileToS3", () => {
  it("refuses a signal that has already aborted, without opening a request", async () => {
    // The case that mattered: cancelling during the presign call leaves a signal that has
    // already fired, so waiting for another `abort` event would upload the whole file anyway.
    const open = vi.fn();
    vi.stubGlobal(
      "XMLHttpRequest",
      class {
        upload = { addEventListener: vi.fn() };
        open = open;
        send = vi.fn();
        addEventListener = vi.fn();
      }
    );

    const controller = new AbortController();
    controller.abort();

    await expect(
      uploadFileToS3(new File(["x"], "a.gpkg"), presigned, { signal: controller.signal })
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(open).not.toHaveBeenCalled();

    vi.unstubAllGlobals();
  });

  it("aborts an in-flight request when the signal fires", async () => {
    const abort = vi.fn();
    let onAbort: (() => void) | undefined;
    vi.stubGlobal(
      "XMLHttpRequest",
      class {
        status = 0;
        upload = { addEventListener: vi.fn() };
        open = vi.fn();
        send = vi.fn();
        abort = abort;
        addEventListener = (event: string, handler: () => void) => {
          if (event === "abort") onAbort = handler;
        };
      }
    );

    const controller = new AbortController();
    const pending = uploadFileToS3(new File(["x"], "a.gpkg"), presigned, {
      signal: controller.signal,
    });
    controller.abort();
    expect(abort).toHaveBeenCalled();
    onAbort?.();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });

    vi.unstubAllGlobals();
  });
});
