import path from "path";

import { expect, test } from "@playwright/test";

const FIXTURES_DIR = path.join(__dirname, "../fixtures/data");

test.describe("Dataset Upload - Non-Spatial Table", () => {
  test("upload a CSV table", async ({ page }) => {
    await page.goto("/datasets");

    // Open upload dialog
    await page.getByRole("button", { name: "Add Dataset" }).click();
    await page.getByRole("menuitem", { name: "Dataset Upload" }).click();

    // Step 1: Select File
    await expect(page.getByRole("heading", { name: "Upload Dataset" })).toBeVisible();

    const fileInput = page.locator('input[type="file"]');
    await fileInput.setInputFiles(path.join(FIXTURES_DIR, "table.csv"));

    const nextButton = page.getByRole("button", { name: "Next" });
    await expect(nextButton).toBeEnabled({ timeout: 5000 });
    await nextButton.click();

    // Step 2: Destination & Metadata
    const nameField = page.getByLabel("Name");
    await expect(nameField).toHaveValue("table");

    const datasetName = `e2e_table_${Date.now()}`;
    await nameField.clear();
    await nameField.fill(datasetName);

    // Folder may be pre-selected but click to ensure
    const folderField = page.getByLabel("Select Folder Destination");
    await folderField.click();
    await page.getByRole("option").first().click();

    await nextButton.click();

    // Step 3: Confirmation
    await expect(page.getByText("Confirmation")).toBeVisible();
    await expect(page.getByText(datasetName)).toBeVisible();

    // Upload
    await page.getByRole("button", { name: "Upload" }).click();

    // Wait for dialog to close
    await expect(page.getByRole("heading", { name: "Upload Dataset" })).not.toBeVisible({
      timeout: 60000,
    });

    // Should still be on datasets page
    await expect(page).toHaveURL(/\/datasets/);
  });
});
