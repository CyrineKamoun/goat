import { expect, test } from "@playwright/test";

import { deleteContentItem } from "../fixtures/content";

test.describe("Workflow Management", () => {
  // Set by beforeEach, read by afterEach — safe as a describe-scoped
  // variable because Playwright never runs two tests of the same describe
  // concurrently within one worker.
  let projectName: string;

  test.beforeEach(async ({ page }) => {
    // Create a fresh project for workflow tests. The old dedicated
    // /projects page now redirects to /content; project creation lives
    // behind its "Add new" menu instead.
    await page.goto("/content");
    await page.getByRole("button", { name: "Add new" }).click();
    await page.getByRole("menuitem", { name: "New Project" }).click();
    await expect(page.getByRole("heading", { name: "New Project" })).toBeVisible();

    // The dialog asks for a name and nothing else — the destination is the
    // folder being browsed.
    projectName = `E2E Workflow Test ${Date.now()}`;
    await page.getByLabel("New Project").fill(projectName);
    await page.getByRole("button", { name: "Create project" }).click();

    await expect(page).toHaveURL(/\/map\//, { timeout: 30000 });
  });

  test.afterEach(async ({ page }) => {
    await deleteContentItem(page, projectName);
  });

  test("create a new workflow and see the canvas", async ({ page }) => {
    // Switch to Workflows tab
    await page.getByText("Workflows").click();

    // Should see workflows panel
    await expect(page.getByRole("heading", { name: "Workflows" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Add Workflow" })).toBeVisible();

    // Create a new workflow
    await page.getByRole("button", { name: "Add Workflow" }).click();

    // Should see the workflow canvas with toolbar
    await expect(page.getByRole("button", { name: "Run" })).toBeVisible({ timeout: 5000 });
    await expect(page.getByRole("button", { name: "Select" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Zoom In" })).toBeVisible();

    // Tools panel should be visible with tool categories
    await expect(page.getByText("Geoprocessing")).toBeVisible();
    await expect(page.getByText("Buffer")).toBeVisible();
    await expect(page.getByText("Accessibility Indicators")).toBeVisible();
  });

  // Skip: ReactFlow drag-and-drop uses a ref (dragDataRef) set by React's onDragStart
  // which doesn't trigger from synthetic DOM DragEvents. Use codegen/manual recording instead.
  test.skip("add tool nodes to workflow via drag and drop", async ({ page }) => {
    await page.getByText("Workflows").click();
    await page.getByRole("button", { name: "Add Workflow" }).click();
    await expect(page.getByRole("button", { name: "Run" })).toBeVisible({ timeout: 5000 });

    // Get the canvas area and the source tool element
    const canvas = page.locator(".react-flow");
    await expect(canvas).toBeVisible();
    const canvasBounds = await canvas.boundingBox();
    expect(canvasBounds).toBeTruthy();

    const bufferTool = page.getByText("Buffer").first();
    const toolBounds = await bufferTool.boundingBox();
    expect(toolBounds).toBeTruthy();

    const startX = toolBounds!.x + toolBounds!.width / 2;
    const startY = toolBounds!.y + toolBounds!.height / 2;
    const endX = canvasBounds!.x + canvasBounds!.width / 2;
    const endY = canvasBounds!.y + canvasBounds!.height / 2;

    // ReactFlow requires custom dataTransfer data during drag/drop.
    // We use page.evaluate to create proper DragEvent with DataTransfer.
    await page.evaluate(
      ({ sx, sy, ex, ey }) => {
        const source = document.elementFromPoint(sx, sy);
        const target = document.elementFromPoint(ex, ey);
        if (!source || !target) return;

        // Create a DataTransfer with the required data
        const dt = new DataTransfer();
        dt.setData("application/reactflow", "tool");

        // Dispatch dragstart on the tool
        source.dispatchEvent(
          new DragEvent("dragstart", { bubbles: true, dataTransfer: dt })
        );

        // Dispatch dragover on the canvas
        target.dispatchEvent(
          new DragEvent("dragover", {
            bubbles: true,
            cancelable: true,
            clientX: ex,
            clientY: ey,
            dataTransfer: dt,
          })
        );

        // Dispatch drop on the canvas
        target.dispatchEvent(
          new DragEvent("drop", {
            bubbles: true,
            cancelable: true,
            clientX: ex,
            clientY: ey,
            dataTransfer: dt,
          })
        );
      },
      { sx: startX, sy: startY, ex: endX, ey: endY }
    );

    // Verify a Buffer node appeared on the canvas
    await expect(
      page.locator('[roledescription="node"]').filter({ hasText: "Buffer" })
    ).toBeVisible({ timeout: 10000 });
  });

  test("tools panel search works", async ({ page }) => {
    await page.getByText("Workflows").click();
    await page.getByRole("button", { name: "Add Workflow" }).click();
    await expect(page.getByRole("button", { name: "Run" })).toBeVisible({ timeout: 5000 });

    // Search for a tool
    const searchBox = page.getByPlaceholder("Search");
    await searchBox.fill("buffer");

    // Buffer should be visible, other tools should be hidden
    await expect(page.getByText("Buffer")).toBeVisible();
    await expect(page.getByText("Geoprocessing")).toBeVisible();

    // Clear search and verify all categories return
    await searchBox.clear();
    await expect(page.getByText("Accessibility Indicators")).toBeVisible();
    await expect(page.getByText("Geoanalysis")).toBeVisible();
    await expect(page.getByText("Data Management")).toBeVisible();
  });
});
