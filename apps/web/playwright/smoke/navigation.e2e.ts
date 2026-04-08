import { expect, test } from "@playwright/test";

test.describe("Navigation & Smoke Tests", () => {
  test("home page loads and shows recent projects", async ({ page }) => {
    await page.goto("/home");
    await expect(page).toHaveTitle(/GOAT/);
    await expect(page.getByRole("heading", { name: "Recent Projects" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Recent Datasets" })).toBeVisible();
  });

  test("can navigate to all main sections", async ({ page }) => {
    await page.goto("/home");

    // Navigate to Projects
    await page.getByRole("link", { name: "Projects" }).click();
    await expect(page.getByRole("heading", { name: "Projects" })).toBeVisible();

    // Navigate to Datasets
    await page.getByRole("link", { name: "Datasets" }).click();
    await expect(page.getByRole("heading", { name: "Datasets" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Add Dataset" })).toBeVisible();

    // Navigate to Catalog
    await page.getByRole("link", { name: "Catalog" }).click();
    await expect(page.getByRole("heading", { name: "Catalog" })).toBeVisible();

    // Navigate to Settings
    await page.getByRole("link", { name: "Settings" }).click();
    await expect(page.getByRole("heading", { name: "Settings" })).toBeVisible();

    // Navigate back Home
    await page.getByRole("link", { name: "Home" }).click();
    await expect(page.getByRole("heading", { name: "Recent Projects" })).toBeVisible();
  });

  test("recent projects and datasets sections have create cards", async ({ page }) => {
    await page.goto("/home");
    await expect(page.getByRole("heading", { name: "Recent Projects" })).toBeVisible();
    // The "+" create cards have aria titles
    await expect(page.locator('[aria-label="Create New Project"]')).toBeVisible();
    await expect(page.locator('[aria-label="Create New Dataset"]')).toBeVisible();
  });
});
