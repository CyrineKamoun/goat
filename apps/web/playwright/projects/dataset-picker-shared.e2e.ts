import { expect, test } from "@playwright/test";

import {
  SHARED_TEAM_NAME,
  deleteContentItem,
  ensureSharedTeam,
  locateContentCard,
} from "../fixtures/content";

/**
 * A dataset shared with a team is offered by the project's Add Layer picker
 * under that team's space, the same way the Content page lists it there.
 *
 * The two surfaces once disagreed: the Content page merged "shared into this
 * space" items on its own, and the picker, built on the same feed, never saw
 * them. What a space contains now comes from the feed itself, and this spec
 * holds the picker to it with real data, across the share dialog, the feed
 * and the picker.
 *
 * The account is expected to already hold a dataset; the spec shares the
 * first one it finds and takes the share back at the end.
 */
test("a dataset shared with a team is offered by the picker under the team's space", async ({ page }) => {
  // Shares, creates a project, picks, then cleans both up: longer than the default.
  test.setTimeout(240_000);
  await ensureSharedTeam(page);

  // Datasets on the Content page carry their layer type as a label.
  await page.goto("/content");
  const datasets = page.locator(".content-card").filter({ hasText: /Feature|Table|Raster/ });
  await expect(datasets.first()).toBeVisible({ timeout: 15000 });
  const datasetName = (await datasets.first().locator("[title]").first().getAttribute("title")) as string;
  expect(datasetName, "a dataset to share").toBeTruthy();

  // Share with the team: Teams tab, the team row's role button, a role, Done.
  const setTeamRole = async (role: RegExp) => {
    const card = await locateContentCard(page, datasetName);
    expect(card, `the dataset "${datasetName}" on the Content page`).not.toBeNull();
    await card!.getByRole("button", { name: "More" }).click();
    await page.getByRole("button", { name: "Share", exact: true }).click();
    await page.getByRole("tab", { name: "Teams" }).click();
    const teamRow = page
      .locator("div")
      .filter({ hasText: SHARED_TEAM_NAME })
      .filter({ has: page.getByRole("button") })
      .last();
    await teamRow.getByRole("button").click();
    await page.getByRole("menuitem", { name: role }).click();
    await page.getByRole("button", { name: "Done" }).click();
    await expect(page.getByRole("button", { name: "Done" })).toBeHidden({ timeout: 10000 });
  };
  await setTeamRole(/^Viewer/);

  const projectName = `E2E Shared Dataset Picker ${Date.now()}`;
  try {
    // A blank project from Home (see dataset-picker.e2e.ts).
    await page.goto("/home");
    await page.getByRole("button", { name: "New project" }).click();
    await page.getByRole("menuitem", { name: "Blank project" }).click();
    await expect(page.getByRole("dialog").getByText("New project", { exact: true })).toBeVisible();
    const nameField = page.getByLabel("New project");
    await nameField.click();
    await nameField.fill(projectName);
    await page.getByRole("button", { name: "Create project" }).click();
    await expect(page).toHaveURL(/\/map\//, { timeout: 30000 });
    await expect(page.getByRole("button", { name: "Add layer" })).toBeVisible({ timeout: 15000 });

    await page.getByRole("button", { name: "Add layer" }).click();
    await page.getByRole("menuitem", { name: "My datasets" }).click();
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByText("My datasets")).toBeVisible();

    // The team's space in the picker lists the shared dataset, pickable.
    await dialog.getByText(SHARED_TEAM_NAME, { exact: true }).click();
    const shared = dialog.locator(".content-card").filter({ hasText: datasetName });
    await expect(shared.first()).toBeVisible({ timeout: 15000 });
    await expect(shared.first().getByRole("checkbox")).toBeVisible();
    await page.keyboard.press("Escape");
  } finally {
    await deleteContentItem(page, projectName);
    await setTeamRole(/^No access/i);
  }
});
