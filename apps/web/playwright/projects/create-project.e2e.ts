import { expect, test } from "@playwright/test";

test.describe("Project Management", () => {
  test("create a new project from home page", async ({ page }) => {
    await page.goto("/home");

    // Click the "Create New Project" card (a div with aria-label)
    await page.locator('[aria-label="Create New Project"]').click();

    // Dialog should appear
    await expect(page.getByRole("heading", { name: "Create project" })).toBeVisible();

    // Fill in project name
    const projectName = `E2E Test Project ${Date.now()}`;
    const nameField = page.getByLabel("Name");
    await nameField.click();
    await nameField.fill(projectName);

    // Select folder location — open the autocomplete dropdown and pick first option
    const folderField = page.getByLabel("Folder location");
    await folderField.click();
    // Wait for the dropdown options to appear
    const firstOption = page.getByRole("option").first();
    await expect(firstOption).toBeVisible({ timeout: 5000 });
    await firstOption.click();

    // Submit
    await page.getByRole("button", { name: "Create" }).click();

    // Should redirect to map view (increase timeout for API call + navigation)
    await expect(page).toHaveURL(/\/map\//, { timeout: 30000 });

    // Map view should load — add layer button visible confirms layer panel loaded
    await expect(page.getByRole("button", { name: "Add layer" })).toBeVisible({ timeout: 15000 });
  });

  test("create project from projects page", async ({ page }) => {
    await page.goto("/projects");

    await page.getByRole("button", { name: "New Project" }).click();
    await expect(page.getByRole("heading", { name: "Create project" })).toBeVisible();

    // Fill in details
    await page.getByLabel("Name").fill(`E2E Project Page ${Date.now()}`);

    const folderField = page.getByLabel("Folder location");
    await folderField.click();
    const firstOption = page.getByRole("option").first();
    await expect(firstOption).toBeVisible({ timeout: 5000 });
    await firstOption.click();

    await page.getByRole("button", { name: "Create" }).click();

    // Should redirect to map
    await expect(page).toHaveURL(/\/map\//, { timeout: 30000 });
  });
});
