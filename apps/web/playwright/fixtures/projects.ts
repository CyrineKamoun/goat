import { type APIRequestContext, expect } from "@playwright/test";

import { API_URL } from "./users";

const INITIAL_VIEW_STATE = {
  latitude: 48.1502132,
  longitude: 11.5696284,
  zoom: 12,
  min_zoom: 0,
  max_zoom: 20,
  bearing: 0,
  pitch: 0,
};

/** Creates a project in the caller's own home folder and returns its id. */
export const createProject = async (api: APIRequestContext, name: string): Promise<string> => {
  const folders = await api.get(`${API_URL}/api/v2/folder`);
  expect(folders.ok(), await folders.text()).toBeTruthy();
  const home = ((await folders.json()) as { id: string; name: string; is_owned: boolean }[]).find(
    (folder) => folder.name === "home" && folder.is_owned
  );
  expect(home, "no owned home folder").toBeTruthy();
  const created = await api.post(`${API_URL}/api/v2/project`, {
    data: { folder_id: home!.id, name, initial_view_state: INITIAL_VIEW_STATE },
  });
  expect(created.ok(), await created.text()).toBeTruthy();
  return ((await created.json()) as { id: string }).id;
};

/** Grants users a role on a project ("project-viewer" / "project-editor"). */
export const shareProjectWithUsers = async (
  api: APIRequestContext,
  projectId: string,
  users: { id: string; role: "project-viewer" | "project-editor" }[]
): Promise<void> => {
  const shared = await api.post(`${API_URL}/api/v2/share/project/${projectId}`, { data: { users } });
  expect(shared.ok(), await shared.text()).toBeTruthy();
};

export const deleteProject = async (api: APIRequestContext, projectId: string | undefined): Promise<void> => {
  if (projectId) await api.delete(`${API_URL}/api/v2/project/${projectId}`);
};
