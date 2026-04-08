import { expect, test } from "@playwright/test";
import * as path from "path";

// Helper: open the three-dot context menu for an item by name
async function openContextMenu(page: import("@playwright/test").Page, itemName: string) {
  const card = page.locator("[class*=MuiCard-root]").filter({ hasText: itemName });
  await card.locator("button").last().click();
}

test.describe("Dataset Management Operations", () => {
  let datasetName: string;

  test.beforeEach(async ({ page }) => {
    // Upload a CSV dataset to operate on
    datasetName = `E2E Dataset ${Date.now()}`;
    await page.goto("/datasets");
    await page.getByRole("button", { name: "Add Dataset" }).click();
    await page.getByRole("menuitem", { name: "Dataset Upload" }).click();

    // Step 1: Select File
    await expect(page.getByRole("heading", { name: "Upload Dataset" })).toBeVisible();
    const fileInput = page.locator('input[type="file"]');
    await fileInput.setInputFiles(
      path.resolve(__dirname, "../fixtures/data/table.csv")
    );
    const nextButton = page.getByRole("button", { name: "Next" });
    await expect(nextButton).toBeEnabled({ timeout: 5000 });
    await nextButton.click();

    // Step 2: Destination & Metadata
    const nameField = page.getByLabel("Name");
    await nameField.clear();
    await nameField.fill(datasetName);

    const folderField = page.getByLabel("Select Folder Destination");
    await folderField.click();
    const homeOption = page.getByRole("option", { name: "home" });
    await expect(homeOption).toBeVisible({ timeout: 5000 });
    await homeOption.click();

    await nextButton.click();

    // Step 3: Confirmation — finish upload
    await page.getByRole("button", { name: "Upload" }).click();

    // Wait for upload dialog to close
    await expect(page.getByRole("heading", { name: "Upload Dataset" })).not.toBeVisible({ timeout: 60000 });

    // Navigate to datasets and wait for the newly created one
    await page.goto("/datasets");
    await expect(page.getByText(datasetName)).toBeVisible({ timeout: 15000 });
  });

  test("edit dataset metadata", async ({ page }) => {
    await openContextMenu(page, datasetName);

    // Click "Edit metadata"
    await page.getByRole("button", { name: "Edit metadata" }).click();

    // Dialog should appear
    await expect(page.getByRole("heading", { name: "Edit metadata" })).toBeVisible();

    // Update the name and description
    const updatedName = `${datasetName} Updated`;
    const nameField = page.getByLabel("Name", { exact: true });
    await nameField.clear();
    await nameField.fill(updatedName);

    const descriptionField = page.getByLabel("Description");
    await descriptionField.fill("E2E test description");

    await page.getByRole("button", { name: "Update" }).click();

    // Verify the updated name is visible
    await expect(page.getByText(updatedName)).toBeVisible({ timeout: 10000 });
  });

  test("move dataset to folder", async ({ page }) => {
    await openContextMenu(page, datasetName);

    // Click "Move to folder"
    await page.getByRole("button", { name: "Move to folder" }).click();

    // Dialog should appear
    await expect(page.getByRole("heading", { name: "Move to" })).toBeVisible();

    // The current folder is pre-selected ("home") — select a different one ("test")
    const folderCombobox = page.getByRole("combobox", { name: "Select Folder Destination" });
    await folderCombobox.clear();
    await folderCombobox.fill("test");
    const testOption = page.getByRole("option", { name: "test" });
    await expect(testOption).toBeVisible({ timeout: 5000 });
    await testOption.click();

    // Move button should now be enabled
    const moveButton = page.getByRole("button", { name: "Move" });
    await expect(moveButton).toBeEnabled({ timeout: 5000 });
    await moveButton.click();

    // Dialog should close
    await expect(page.getByRole("heading", { name: "Move to" })).not.toBeVisible({ timeout: 5000 });
  });

  test("delete dataset", async ({ page }) => {
    await openContextMenu(page, datasetName);

    // Click "Delete"
    await page.getByRole("button", { name: "Delete" }).click();

    // Confirmation dialog should appear (heading is "Delete Layer" for datasets)
    await expect(page.getByRole("heading", { name: "Delete Layer" })).toBeVisible();

    // Confirm deletion (click the Delete button inside the dialog)
    await page.getByLabel("Delete Layer").getByRole("button", { name: "Delete" }).click();

    // Wait for dialog to close, then verify dataset is gone from the list
    await expect(page.getByRole("heading", { name: "Delete Layer" })).not.toBeVisible({ timeout: 10000 });
    await expect(page.locator("main").getByText(datasetName)).not.toBeVisible({ timeout: 10000 });
  });
});
