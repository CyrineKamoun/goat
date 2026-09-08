import path from "path";

import { expect, test } from "@playwright/test";

import { tryDeleteContentItem } from "../fixtures/content";

const FIXTURES_DIR = path.join(__dirname, "../fixtures/data");

test.describe("Dataset Upload - Non-Spatial Table", () => {
  test("upload a CSV table", async ({ page }) => {
    await page.goto("/content");

    // Add new -> Dataset opens the upload dialog directly.
    await page.getByRole("button", { name: "Add new" }).click();
    await page.getByRole("menuitem", { name: "Dataset" }).click();

    await expect(page.getByRole("dialog").getByText("Upload file", { exact: true })).toBeVisible();

    const fileInput = page.locator('input[type="file"]');
    await fileInput.setInputFiles(path.join(FIXTURES_DIR, "table.csv"));

    const nameField = page.getByRole("textbox", { name: "Layer name" });
    await expect(nameField).toHaveValue("table");
    const datasetName = `e2e_table_${Date.now()}`;
    await nameField.fill(datasetName);

    // Same environment limit as the spatial-upload spec: geoapi/processes
    // are not running, so this stops at submission rather than waiting for
    // the table to finish importing (the dialog closes as soon as the
    // upload is handed off — see useUploadFlow.submit).
    const uploadButton = page.getByRole("button", { name: "Upload" });
    await expect(uploadButton).toBeEnabled();
    await uploadButton.click();

    await expect(page.getByRole("dialog").getByText("Upload file", { exact: true })).toBeHidden({
      timeout: 10000,
    });

    await tryDeleteContentItem(page, datasetName);
  });
});
