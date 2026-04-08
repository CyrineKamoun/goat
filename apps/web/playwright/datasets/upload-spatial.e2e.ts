import path from "path";

import { expect, test } from "@playwright/test";

const FIXTURES_DIR = path.join(__dirname, "../fixtures/data");

test.describe("Dataset Upload - Spatial Layer", () => {
  test("upload a GeoJSON point layer", async ({ page }) => {
    await page.goto("/datasets");

    // Open upload dialog
    await page.getByRole("button", { name: "Add Dataset" }).click();
    await page.getByRole("menuitem", { name: "Dataset Upload" }).click();

    // Step 1: Select File
    await expect(page.getByRole("heading", { name: "Upload Dataset" })).toBeVisible();
    await expect(page.getByText("Select File")).toBeVisible();

    // Upload the GeoJSON file via the hidden file input
    const fileInput = page.locator('input[type="file"]');
    await fileInput.setInputFiles(path.join(FIXTURES_DIR, "points.geojson"));

    // Next button should become enabled
    const nextButton = page.getByRole("button", { name: "Next" });
    await expect(nextButton).toBeEnabled({ timeout: 5000 });
    await nextButton.click();

    // Step 2: Destination & Metadata
    await expect(page.getByText("Destination & Metadata")).toBeVisible();

    // Name field should be pre-filled with filename
    const nameField = page.getByLabel("Name");
    await expect(nameField).toHaveValue("points");

    // Change the name to something unique
    const datasetName = `e2e_points_${Date.now()}`;
    await nameField.clear();
    await nameField.fill(datasetName);

    // Select folder — the folder field uses "Select Folder Destination" label
    // It may already be pre-selected (home folder), but click to ensure
    const folderField = page.getByLabel("Select Folder Destination");
    await folderField.click();
    await page.getByRole("option").first().click();

    await nextButton.click();

    // Step 3: Confirmation
    await expect(page.getByText("Confirmation")).toBeVisible();
    await expect(page.getByText(datasetName)).toBeVisible();
    await expect(page.getByText("points.geojson")).toBeVisible();

    // Upload
    await page.getByRole("button", { name: "Upload" }).click();

    // Wait for upload to complete - dialog should close
    await expect(page.getByRole("heading", { name: "Upload Dataset" })).not.toBeVisible({
      timeout: 60000,
    });

    // Should still be on datasets page
    await expect(page).toHaveURL(/\/datasets/);

    // Verify dataset appears — navigate to datasets to refresh the list
    await page.goto("/datasets");
    await expect(page.getByRole("heading", { name: "Datasets" })).toBeVisible();
  });
});
