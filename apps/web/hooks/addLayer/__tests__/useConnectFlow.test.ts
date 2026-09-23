import { act, renderHook, waitFor } from "@testing-library/react";
import { readFileSync } from "fs";
import path from "path";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useConnectFlow } from "@/hooks/addLayer/useConnectFlow";

vi.mock("react-i18next", () => ({ useTranslation: () => ({ t: (key: string) => key }) }));
vi.mock("react-toastify", () => ({ toast: { success: vi.fn(), info: vi.fn(), error: vi.fn() } }));
vi.mock("@/lib/api/processes", () => ({ useJobs: () => ({ mutate: vi.fn() }) }));
vi.mock("@/lib/api/teams", () => ({ useTeams: () => ({ teams: [], isLoading: false }) }));
vi.mock("@/lib/api/content", () => ({ refreshContentFeed: vi.fn() }));

const FOLDER = { id: "3fa85f64-5717-4562-b3fc-2c963f66afa6", name: "Stadtklima", is_owned: true };
vi.mock("@/lib/api/folders", () => ({
  useFolders: () => ({ folders: [FOLDER] }),
  getWritableFolders: (folders: unknown[]) => folders,
}));

const addedToProject: unknown[] = [];
vi.mock("@/lib/api/projects", () => ({
  useProject: () => ({ project: { folder_id: FOLDER.id }, isLoading: false, mutate: vi.fn() }),
  useProjectLayers: () => ({ mutate: vi.fn() }),
  addProjectLayers: (projectId: string, ids: string[]) => {
    addedToProject.push({ projectId, ids });
    return Promise.resolve();
  },
}));

const dispatched: unknown[] = [];
vi.mock("@/hooks/store/ContextHooks", () => ({
  useAppDispatch: () => (action: unknown) => dispatched.push(action),
  useAppSelector: () => [],
}));

const rasterCreated: unknown[] = [];
const imported: unknown[] = [];
vi.mock("@/lib/api/layers", () => ({
  createRasterLayer: (payload: unknown, projectId?: string) => {
    rasterCreated.push({ payload, projectId });
    return Promise.resolve({ id: "layer-1" });
  },
  createLayer: (payload: unknown, projectId?: string) => {
    imported.push({ payload, projectId });
    return Promise.resolve({ jobID: "job-1" });
  },
}));

const fixture = (name: string) =>
  readFileSync(path.join(__dirname, "../../../lib/utils/__tests__/fixtures/capabilities", name), "utf8");

const respondWith = (body: string, ok = true) =>
  vi.fn().mockResolvedValue({ ok, text: () => Promise.resolve(body) } as Response);

const connectTo = async (result: { current: ReturnType<typeof useConnectFlow> }, address: string) => {
  act(() => result.current.connect.setAddress(address));
  await act(async () => {
    await result.current.connect.connect();
  });
};

describe("useConnectFlow", () => {
  beforeEach(() => {
    rasterCreated.length = 0;
    imported.length = 0;
    addedToProject.length = 0;
    dispatched.length = 0;
  });

  it("starts idle, with nothing to add", () => {
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    expect(result.current.connect.status).toBe("idle");
    expect(result.current.action).toMatchObject({ label: "add_layer", disabled: true });
  });

  it("takes a tile template without fetching anything, named after its host", async () => {
    global.fetch = vi.fn();
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    await connectTo(result, "https://tile.openstreetmap.de/{z}/{x}/{y}.png");

    expect(global.fetch).not.toHaveBeenCalled();
    expect(result.current.connect.service?.kind).toBe("xyz");
    expect(result.current.connect.name).toBe("tile.openstreetmap.de");
    await waitFor(() => expect(result.current.connect.selectedFolder?.id).toBe(FOLDER.id));
    expect(result.current.action.disabled).toBe(false);
  });

  it("reads a WMS, combines picked layers and adds them to the project", async () => {
    global.fetch = respondWith(fixture("wms-1.3.0.xml"));
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    await connectTo(result, "https://www.wms.nrw.de/geobasis/wms_nw_dop");

    expect(global.fetch).toHaveBeenCalledWith(
      "https://www.wms.nrw.de/geobasis/wms_nw_dop?SERVICE=WMS&REQUEST=GetCapabilities",
      expect.anything()
    );
    expect(result.current.connect.status).toBe("connected");
    expect(result.current.action.disabled).toBe(true);
    expect(result.current.action.reason).toBe("connect_service_pick_layers");

    act(() => result.current.connect.toggleLayer("nw_dop_rgb"));
    act(() => result.current.connect.toggleLayer("nw_dop_cir"));
    expect(result.current.connect.selected).toEqual(["nw_dop_rgb", "nw_dop_cir"]);
    expect(result.current.connect.name).toBe("DOP colour + DOP colour infrared");

    await waitFor(() => expect(result.current.action.disabled).toBe(false));
    await act(async () => {
      await result.current.action.run();
    });
    expect(rasterCreated).toHaveLength(1);
    expect(rasterCreated[0]).toMatchObject({
      projectId: "p1",
      payload: {
        data_type: "wms",
        folder_id: FOLDER.id,
        other_properties: { layers: ["nw_dop_rgb", "nw_dop_cir"] },
      },
    });
    expect(addedToProject).toEqual([{ projectId: "p1", ids: ["layer-1"] }]);
  });

  it("ignores a layer GOAT cannot draw", async () => {
    global.fetch = respondWith(fixture("wms-1.3.0.xml"));
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    await connectTo(result, "https://www.wms.nrw.de/geobasis/wms_nw_dop");
    act(() => result.current.connect.toggleLayer("nw_dop_nir"));
    expect(result.current.connect.selected).toEqual([]);
  });

  it("keeps a user's own name over the suggested one", async () => {
    global.fetch = respondWith(fixture("wms-1.3.0.xml"));
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    await connectTo(result, "https://www.wms.nrw.de/geobasis/wms_nw_dop");
    act(() => result.current.connect.setName("Luftbild"));
    act(() => result.current.connect.toggleLayer("nw_dop_rgb"));
    expect(result.current.connect.name).toBe("Luftbild");
  });

  it("imports a WFS feature type as a job, one at a time", async () => {
    global.fetch = respondWith(fixture("wfs-2.0.0.xml"));
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    await connectTo(
      result,
      "https://www.wfs.nrw.de/geobasis/wfs_nw_alkis_vereinfacht?SERVICE=WFS&REQUEST=GetCapabilities"
    );
    act(() => result.current.connect.toggleLayer("ave:Flurstueck"));
    act(() => result.current.connect.toggleLayer("ave:GebaeudeBauwerk"));
    expect(result.current.connect.selected).toEqual(["ave:GebaeudeBauwerk"]);
    expect(result.current.action.label).toBe("connect_service_import");

    await waitFor(() => expect(result.current.action.disabled).toBe(false));
    await act(async () => {
      await result.current.action.run();
    });
    expect(imported).toHaveLength(1);
    expect(imported[0]).toMatchObject({
      projectId: "p1",
      payload: { data_type: "wfs", other_properties: { layers: ["ave:GebaeudeBauwerk"] } },
    });
    expect(dispatched).toHaveLength(1);
    expect(rasterCreated).toHaveLength(0);
  });

  it("says a service is unreachable when the request fails, as a CORS refusal does", async () => {
    global.fetch = vi.fn().mockRejectedValue(new TypeError("Failed to fetch"));
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    await connectTo(result, "https://maps.example.org/ows?SERVICE=WMS&REQUEST=GetCapabilities");
    expect(result.current.connect.status).toBe("failed");
    expect(result.current.connect.failure).toBe("unreachable");
  });

  it("says an address is not a service when the answer is something else", async () => {
    global.fetch = respondWith("<html><body>Geoportal</body></html>");
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    await connectTo(result, "https://www.stadt-beispiel.de/geoportal");
    expect(result.current.connect.failure).toBe("unrecognised");
  });

  it("goes back to idle when the address changes, so nothing stale can be added", async () => {
    global.fetch = respondWith(fixture("wms-1.3.0.xml"));
    const { result } = renderHook(() => useConnectFlow({ projectId: "p1" }));
    await connectTo(result, "https://www.wms.nrw.de/geobasis/wms_nw_dop");
    act(() => result.current.connect.toggleLayer("nw_dop_rgb"));
    act(() => result.current.connect.setAddress("https://other.example/wms"));
    expect(result.current.connect.status).toBe("idle");
    expect(result.current.connect.service).toBeUndefined();
    expect(result.current.connect.selected).toEqual([]);
  });
});
