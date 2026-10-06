import { expect, test } from "@playwright/test";

import { datasetOf } from "../fixtures/datasets";
import { addDatasetsToProject, createProject, deleteProject } from "../fixtures/projects";
import { apiAs, needsAuth } from "../fixtures/users";

/**
 * Printing a layout to PDF: the print worker opens the layout in its own
 * browser, renders it and stores the file, which the jobs menu then hands
 * to this browser.
 */
needsAuth(test);
// Jobs run in Windmill and take longer than the suite's default timeout.
test.describe.configure({ timeout: 360000 });

let projectId: string;

test.beforeAll(async () => {
  const api = await apiAs("owner");
  try {
    projectId = await createProject(api, `E2E Print ${Date.now()}`);
    await addDatasetsToProject(api, projectId, [datasetOf("points")]);
  } finally {
    await api.dispose();
  }
});

test.afterAll(async () => {
  const api = await apiAs("owner");
  try {
    await deleteProject(api, projectId);
  } finally {
    await api.dispose();
  }
});

test("a new layout prints to a PDF", async ({ page }) => {
  await page.goto(`/map/${projectId}?mode=reports`);
  await page.getByRole("button", { name: "New", exact: true }).click({ timeout: 30000 });
  await page.getByRole("menuitem", { name: "From scratch" }).click();

  const print = page.getByRole("button", { name: "Print Layout" });
  await expect(print).toBeEnabled();
  // Stay on the page until the file arrives: the jobs menu only downloads
  // jobs it saw running.
  const download = page.waitForEvent("download", { timeout: 300000 });
  await print.click();

  const file = await download;
  expect(file.suggestedFilename()).toMatch(/\.pdf$/);
  expect(await file.failure()).toBeNull();
});
