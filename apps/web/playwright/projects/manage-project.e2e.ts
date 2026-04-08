import { expect, test } from "@playwright/test";

// Helper: open the three-dot context menu for an item by name
async function openContextMenu(page: import("@playwright/test").Page, itemName: string) {
  // The card has a Stack with the name text and three-dot IconButton as siblings.
  // We locate the card containing our text and click the IconButton within the same row.
  const card = page.locator("[class*=MuiCard-root]").filter({ hasText: itemName });
  await card.locator("button").last().click();
}

test.describe("Project Management Operations", () => {
  let projectName: string;

  test.beforeEach(async ({ page }) => {
    // Create a project to operate on
    projectName = `E2E Manage ${Date.now()}`;
    await page.goto("/projects");
    await page.getByRole("button", { name: "New Project" }).click();
    await expect(page.getByRole("heading", { name: "Create project" })).toBeVisible();

    await page.getByLabel("Name").fill(projectName);
    const folderField = page.getByLabel("Folder location");
    await folderField.click();
    // Select "home" folder explicitly (first option may vary)
    const homeOption = page.getByRole("option", { name: "home" });
    await expect(homeOption).toBeVisible({ timeout: 5000 });
    await homeOption.click();
    await page.getByRole("button", { name: "Create" }).click();

    // Wait for redirect to map, then go back to projects list
    await expect(page).toHaveURL(/\/map\//, { timeout: 30000 });
    await page.goto("/projects");

    // The default view shows the "home" folder — our project should be visible
    await expect(page.getByText(projectName)).toBeVisible({ timeout: 15000 });
  });

  test("edit project metadata", async ({ page }) => {
    await openContextMenu(page, projectName);

    // Click "Edit metadata"
    await page.getByRole("button", { name: "Edit metadata" }).click();

    // Dialog should appear with pre-filled name
    await expect(page.getByRole("heading", { name: "Edit metadata" })).toBeVisible();

    // Update the name and description
    const updatedName = `${projectName} Updated`;
    const nameField = page.getByLabel("Name");
    await nameField.clear();
    await nameField.fill(updatedName);

    const descriptionField = page.getByLabel("Description");
    await descriptionField.fill("E2E test description");

    await page.getByRole("button", { name: "Update" }).click();

    // Verify the updated name is visible
    await expect(page.getByText(updatedName)).toBeVisible({ timeout: 10000 });
  });

  test("move project to folder", async ({ page }) => {
    await openContextMenu(page, projectName);

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

  test("delete project", async ({ page }) => {
    await openContextMenu(page, projectName);

    // Click "Delete"
    await page.getByRole("button", { name: "Delete" }).click();

    // Confirmation dialog should appear
    await expect(page.getByRole("heading", { name: "Delete Project" })).toBeVisible();

    // Confirm deletion (click the Delete button inside the dialog)
    await page.getByLabel("Delete Project").getByRole("button", { name: "Delete" }).click();

    // Wait for dialog to close, then verify project is gone from the list
    await expect(page.getByRole("heading", { name: "Delete Project" })).not.toBeVisible({ timeout: 10000 });
    await expect(page.locator("main").getByText(projectName)).not.toBeVisible({ timeout: 10000 });
  });
});
