import path from "path";

import { expect, test } from "@playwright/test";

import { tryDeleteContentItem } from "../fixtures/content";

const FIXTURES_DIR = path.join(__dirname, "../fixtures/data");

test.describe("Dataset Upload - Spatial Layer", () => {
  test("upload a GeoJSON point layer", async ({ page }) => {
    await page.goto("/content");

    // Add new -> Dataset opens the upload dialog itself: a file upload is
    // the only way to bring data into a space from this page (the
    // catalog/create sources belong to the map builder, where a layer is
    // added to a project).
    await page.getByRole("button", { name: "Add new" }).click();
    await page.getByRole("menuitem", { name: "Dataset" }).click();

    await expect(page.getByRole("dialog").getByText("Upload file", { exact: true })).toBeVisible();

    // Upload the GeoJSON file via the hidden file input the dropzone wraps.
    const fileInput = page.locator('input[type="file"]');
    await fileInput.setInputFiles(path.join(FIXTURES_DIR, "points.geojson"));

    // The name is suggested from the filename and the destination folder
    // from the folder being browsed (My Content's root here), so the only
    // thing left to set is a unique name.
    const nameField = page.getByRole("textbox", { name: "Layer name" });
    await expect(nameField).toHaveValue("points");
    const datasetName = `e2e_points_${Date.now()}`;
    await nameField.fill(datasetName);

    // geoapi/processes are not running in this environment, so the job that
    // would actually turn this file into a queryable layer can never
    // complete. That does not block this dialog: `useUploadFlow.submit`
    // hands the file to `importDataset` without awaiting it and closes
    // immediately, so this test stops at the point the upload job is
    // submitted rather than waiting on a pipeline this environment can't run.
    const uploadButton = page.getByRole("button", { name: "Upload" });
    await expect(uploadButton).toBeEnabled();
    await uploadButton.click();

    await expect(page.getByRole("dialog").getByText("Upload file", { exact: true })).toBeHidden({
      timeout: 10000,
    });

    // Best-effort: the layer's metadata row may or may not have made it in
    // before its processing job (which cannot complete here) is asked to
    // run — clean it up if it did.
    await tryDeleteContentItem(page, datasetName);
  });
});
