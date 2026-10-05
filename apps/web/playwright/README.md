# End-to-end tests

Playwright specs that drive the real app in a browser: content management,
project creation, the dataset picker, templates, workflows, Home. The config
is `playwright.config.ts` at the repo root.

## Where they run

- **CI:** `.github/workflows/e2e.yml`, nightly at 02:30 UTC, on demand
  ("Run workflow" in the Actions tab), and on PRs that change these specs,
  the seed or the workflow. It starts the infra from the root `compose.yaml`,
  core, geoapi, processes and catalog from source, and a production build of
  the web app, all with `AUTH=False`. A failed run uploads the Playwright
  report and every service's log as the `e2e-report` artifact.
- **Locally:** against a running app on port 3000, for example
  `pnpm exec playwright test --config=playwright.config.ts` from the repo
  root (pass the config explicitly when running from a worktree).

## What the specs assume

- `AUTH=False`: every request is the built-in default user (first name
  "GOAT"), so nothing logs in.
- Two datasets owned by that user, one of them a feature layer. A fresh
  database gets them from `python -m core.scripts.seed_e2e` (in `apps/core`,
  after `initial_data`); the CI stack does not run Windmill, so they cannot be
  created through the app.
- The specs create what else they need (projects, folders, the shared team
  "Design QA") and remove it again, except the team, which stays on purpose.

## Not covered

Anything that needs Windmill: dataset imports, tool runs, workflow runs,
thumbnails, printing. The upload specs only check that the upload dialog
submits.
