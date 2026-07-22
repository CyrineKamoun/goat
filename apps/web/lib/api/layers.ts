import useSWR from "swr";

import { apiRequestAuth, fetcher } from "@/lib/api/fetcher";
import { type Job, PROCESSES_API_BASE_URL, executeProcessAsync } from "@/lib/api/processes";
import { GEOAPI_BASE_URL } from "@/lib/constants";
import type { PaginatedQueryParams } from "@/lib/validations/common";
import type {
  CatalogDatasetGrouped,
  CatalogLayerSummary,
  ClassBreaks,
  CreateLayerFromDataset,
  CreateRasterLayer,
  DatasetCollectionItems,
  DatasetDownloadRequest,
  DatasetMetadataAggregated,
  FieldKind,
  GetCollectionItemsQueryParams,
  GetDatasetSchema,
  GetLayerUniqueValuesQueryParams,
  Layer,
  LayerClassBreaks,
  LayerPaginated,
  LayerQueryables,
  LayerUniqueValuesPaginated,
  PostDataset,
} from "@/lib/validations/layer";

export const LAYERS_API_BASE_URL = new URL("api/v2/layer", process.env.NEXT_PUBLIC_API_URL).href;
export const CATALOG_API_BASE_URL = `${GEOAPI_BASE_URL}/catalog/records/collections/datasets`;
export const COLLECTIONS_API_BASE_URL = `${GEOAPI_BASE_URL}/collections`;

export const updateBaseLayerProperties = async (
  layerId: string,
  properties: Record<string, unknown>
): Promise<void> => {
  const response = await apiRequestAuth(`${LAYERS_API_BASE_URL}/${layerId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties }),
  });
  if (!response.ok) {
    throw new Error(`Failed to update layer properties for ${layerId}`);
  }
};

// ---------------------------------------------------------------------------
// OGC API Records adapters — translate frontend filter/pagination shape
// to OGC query params, and OGC FeatureCollection responses back to the
// flat paginated Layer shape consumers already expect.
// ---------------------------------------------------------------------------

const buildOgcQuery = (
  payload?: GetDatasetSchema,
  pagination?: PaginatedQueryParams
): URLSearchParams => {
  const qs = new URLSearchParams();
  if (payload) {
    if (payload.search) qs.append("q", payload.search);
    payload.type?.forEach((v) => qs.append("type", v));
    payload.license?.forEach((v) => qs.append("license", v));
    payload.data_category?.forEach((v) => qs.append("themes", v));
    payload.geographical_code?.forEach((v) => qs.append("geographical_code", v));
    payload.language_code?.forEach((v) => qs.append("language", v));
    payload.distributor_name?.forEach((v) => qs.append("publisher", v));
    if (payload.spatial_search) qs.append("bbox", payload.spatial_search);
    if (payload.spatial_boost) qs.append("bbox_boost", payload.spatial_boost);
    if (payload.datetime) qs.append("datetime", payload.datetime);
  }
  if (pagination) {
    const size = pagination.size ?? 10;
    const page = pagination.page ?? 1;
    qs.set("limit", String(size));
    qs.set("offset", String((page - 1) * size));
    if (pagination.order_by) {
      const prefix = pagination.order === "ascendent" ? "+" : "-";
      // Map backend column names to OGC sortable properties.
      const sortMap: Record<string, string> = {
        updated_at: "updated",
        created_at: "created",
        name: "title",
        type: "type",
      };
      const ogcKey = sortMap[pagination.order_by] ?? pagination.order_by;
      qs.set("sortby", `${prefix}${ogcKey}`);
    }
  }
  return qs;
};

type ItemLink = {
  rel?: string;
  href?: string;
  title?: string;
  "goat:layerType"?: string;
  "goat:geometryType"?: string;
};
// Group members are rel="item" links (OGC-standard). The layer id is the last
// path segment of the href.
const featureDistributions = (feature: Record<string, unknown>): CatalogLayerSummary[] => {
  const links = (feature.links as ItemLink[] | undefined) ?? [];
  return links
    .filter((l) => l.rel === "item")
    .map((l) => ({
      id: (l.href ?? "").split("/").pop() ?? "",
      name: String(l.title ?? ""),
      type: l["goat:layerType"] ?? null,
      feature_layer_geometry_type: l["goat:geometryType"] ?? null,
    }));
};

type RecordContact = { name?: string; roles?: string[]; emails?: { value?: string }[] };
const publisherOf = (props: Record<string, unknown>): RecordContact | undefined => {
  const contacts = (props.contacts as RecordContact[] | undefined) ?? [];
  return contacts.find((c) => (c.roles ?? []).includes("publisher")) ?? contacts[0];
};
const languageCodeOf = (props: Record<string, unknown>): string | null =>
  ((props.language as { code?: string } | undefined)?.code ??
    (typeof props.language === "string" ? props.language : null)) as string | null;

const featureToLayer = (feature: Record<string, unknown>): Layer => {
  const props = (feature.properties as Record<string, unknown>) ?? {};
  const publisher = publisherOf(props);
  const themes = props.themes as Array<{ concepts?: Array<{ id?: string }> }> | undefined;
  const distributions = featureDistributions(feature);
  const firstDist = distributions[0] ?? ({} as CatalogLayerSummary);
  return {
    id: feature.id as string,
    name: (props.title as string) ?? "",
    description: (props.description as string) ?? null,
    thumbnail_url: (props.thumbnail_url as string) ?? null,
    // Standalone layer: operational type/geometry ride as goat: extras (no item
    // links). Group: fall back to the first member.
    type: (props["goat:layerType"] as string) ?? firstDist.type ?? "feature",
    feature_layer_geometry_type:
      (props["goat:geometryType"] as string) ?? firstDist.feature_layer_geometry_type ?? null,
    data_category: themes?.[0]?.concepts?.[0]?.id ?? null,
    distributor_name: publisher?.name ?? null,
    geographical_code: ((props["goat:geographical_code"] ?? props.geographical_code) as string) ?? null,
    language_code: languageCodeOf(props),
    license: (props.license as string) ?? null,
    tags: (props.keywords as string[]) ?? [],
    other_properties: { ...props, distributions },
    extent: null,
    in_catalog: true,
  } as unknown as Layer;
};

const featureToGroup = (feature: Record<string, unknown>): CatalogDatasetGrouped => {
  const props = (feature.properties as Record<string, unknown>) ?? {};
  const publisher = publisherOf(props);
  const themes = props.themes as Array<{ concepts?: Array<{ id?: string }> }> | undefined;
  return {
    package_id: String(feature.id ?? ""),
    name: (props.title as string) ?? "",
    description: (props.description as string) ?? null,
    data_category: themes?.[0]?.concepts?.[0]?.id ?? null,
    distributor_name: publisher?.name ?? null,
    language_code: languageCodeOf(props),
    license: (props.license as string) ?? null,
    record_jsonb: {
      properties: props,
      links: (feature.links as unknown[]) ?? undefined,
      time: (feature.time as unknown) ?? undefined,
    },
    layers: featureDistributions(feature),
  };
};

const ogcItemsFetcher = async ([url, payload, pagination]: [
  string,
  GetDatasetSchema | undefined,
  PaginatedQueryParams | undefined,
]): Promise<LayerPaginated> => {
  const qs = buildOgcQuery(payload, pagination);
  const res = await apiRequestAuth(`${url}?${qs.toString()}`, { method: "GET" });
  if (!res.ok) {
    const err: Error & { status?: number } = new Error("Failed to load catalog");
    err.status = res.status;
    throw err;
  }
  const body = await res.json();
  const features = (body.features as Array<Record<string, unknown>>) ?? [];
  const total = (body.numberMatched as number) ?? features.length;
  const size = pagination?.size ?? 10;
  const page = pagination?.page ?? 1;
  return {
    items: features.map(featureToLayer),
    total,
    page,
    size,
    pages: size > 0 ? Math.ceil(total / size) : 1,
  };
};

const ogcAggregatesFetcher = async ([url, payload]: [
  string,
  GetDatasetSchema | undefined,
]): Promise<DatasetMetadataAggregated> => {
  const qs = buildOgcQuery(payload);
  const res = await apiRequestAuth(`${url}?${qs.toString()}`, { method: "GET" });
  if (!res.ok) {
    const err: Error & { status?: number } = new Error("Failed to load catalog aggregates");
    err.status = res.status;
    throw err;
  }
  // OGC API - Records - Part 2: Facets shape → flat {field: [{value,count}]}.
  const json = (await res.json()) as {
    facets?: Record<string, { buckets?: { value: string; count: number }[] }>;
  };
  const facets = json.facets ?? {};
  const out: Record<string, { value: string; count: number }[]> = {};
  for (const [key, facet] of Object.entries(facets)) out[key] = facet.buckets ?? [];
  return out as unknown as DatasetMetadataAggregated;
};

/**
 * Fetcher for OGC API Processes execution endpoints (POST requests)
 */
const processExecuteFetcher = async ([url, body]: [string, object]) => {
  const response = await apiRequestAuth(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail?.detail || error.detail || "Process execution failed");
  }
  return response.json();
};

export const useLayers = (queryParams?: PaginatedQueryParams, payload: GetDatasetSchema = {}) => {
  const { data, isLoading, error, mutate, isValidating } = useSWR<LayerPaginated>(
    [`${LAYERS_API_BASE_URL}`, queryParams, payload],
    fetcher
  );
  return {
    layers: data,
    isLoading: isLoading,
    isError: error,
    mutate,
    isValidating,
  };
};

export const useCatalogLayers = (queryParams?: PaginatedQueryParams, payload: GetDatasetSchema = {}) => {
  const { data, isLoading, error, mutate, isValidating } = useSWR<LayerPaginated>(
    [`${CATALOG_API_BASE_URL}/items`, payload, queryParams],
    ogcItemsFetcher
  );
  return {
    layers: data,
    isLoading: isLoading,
    isError: error,
    mutate,
    isValidating,
  };
};

export const useMetadataAggregated = (payload: GetDatasetSchema = {}) => {
  const { data, isLoading, error, mutate } = useSWR<DatasetMetadataAggregated>(
    [`${CATALOG_API_BASE_URL}/items/aggregates`, payload],
    ogcAggregatesFetcher
  );
  return { metadata: data, isLoading, isError: error, mutate };
};

export interface NutsRegion {
  nuts_id: string;
  nuts_name: string;
  level: number;
  country: string;
  bbox: [number, number, number, number];
}

export const useNutsSearch = (query?: string, level?: number, limit = 20) => {
  const params = new URLSearchParams();
  if (query && query.trim()) params.set("q", query.trim());
  if (level !== undefined) params.set("level", String(level));
  params.set("limit", String(limit));
  const url = `${GEOAPI_BASE_URL}/catalog/records/nuts?${params.toString()}`;
  const { data, isLoading, error, mutate } = useSWR<NutsRegion[]>(url, async (u: string) => {
    const res = await apiRequestAuth(u, { method: "GET" });
    if (!res.ok) {
      const err: Error & { status?: number } = new Error("Failed to load NUTS regions");
      err.status = res.status;
      throw err;
    }
    return res.json();
  });
  return { regions: data ?? [], isLoading, isError: error, mutate };
};

export const useCatalogGroup = (groupId?: string) => {
  const url = groupId ? `${CATALOG_API_BASE_URL}/items/${groupId}` : null;
  const { data, isLoading, error, mutate } = useSWR<CatalogDatasetGrouped>(url, async (u: string) => {
    const res = await apiRequestAuth(u, { method: "GET" });
    if (!res.ok) {
      const err: Error & { status?: number } = new Error("Failed to load catalog group");
      err.status = res.status;
      throw err;
    }
    const feature = await res.json();
    return featureToGroup(feature);
  });
  return { group: data, isLoading, isError: error, mutate };
};

// `useCatalogDatasetAsLayer` is the only catalog write flow remaining — used by
// CatalogExplorer modal to add a catalog dataset to a project. The legacy
// `/layer/catalog/{id}/use` endpoint was removed; the modal needs to be reworked
// to either (a) directly link the existing customer.layer row into the project,
// or (b) wait for a replacement OGC-style import flow. Until then this stub
// prevents the import error and surfaces a clear runtime message.
export const useCatalogDatasetAsLayer = async (
  _datasetId: string,
  _payload: { folder_id: string }
): Promise<{ status: string; layer: Layer; use_data?: Record<string, unknown> }> => {
  throw new Error("Catalog dataset import is no longer supported by this endpoint.");
};

export const useDataset = (datasetId: string) => {
  const { data, isLoading, error, mutate } = useSWR<Layer>(
    () => (datasetId ? [`${LAYERS_API_BASE_URL}/${datasetId}`] : null),
    fetcher
  );
  return { dataset: data, isLoading, isError: error, mutate };
};

export const getDataset = async (datasetId: string): Promise<Layer> => {
  // The reason why getDataset is used instead of useDataset is when you want to get the data inside a function
  const response = await apiRequestAuth(`${LAYERS_API_BASE_URL}/${datasetId}`, {
    method: "GET",
  });
  if (!response.ok) {
    throw new Error("Failed to get dataset");
  }
  return await response.json();
};

export const updateDataset = async (datasetId: string, payload: PostDataset) => {
  const response = await apiRequestAuth(`${LAYERS_API_BASE_URL}/${datasetId}`, {
    method: "PUT",
    body: JSON.stringify(payload),
    headers: {
      "Content-Type": "application/json",
    },
  });
  if (!response.ok) {
    await response.json();
  }
  return response;
};

export const updateLayerDataset = async (
  layerId: string,
  options?: { s3_key?: string; refresh_wfs?: boolean }
): Promise<Job> => {
  // Use LayerUpdate process
  // user_id is extracted from JWT token by the server
  const inputs: Record<string, unknown> = {
    layer_id: layerId,
    ...(options?.s3_key && { s3_key: options.s3_key }),
    ...(options?.refresh_wfs && { refresh_wfs: options.refresh_wfs }),
  };

  return executeProcessAsync("layer_update", inputs);
};

export const useDatasetCollectionItems = (datasetId: string, queryParams?: GetCollectionItemsQueryParams) => {
  const { data, isLoading, error, mutate } = useSWR<DatasetCollectionItems>(
    () => (datasetId ? [`${COLLECTIONS_API_BASE_URL}/${datasetId}/items`, queryParams] : null),
    fetcher
  );
  return { data, isLoading, isError: error, mutate };
};

export const useLayerQueryables = (layerId: string) => {
  const { data, isLoading, error, mutate } = useSWR<LayerQueryables>(
    () => (layerId ? [`${COLLECTIONS_API_BASE_URL}/${layerId}/queryables`] : null),
    fetcher
  );
  return { queryables: data, isLoading, isError: error, mutate };
};

//TODO: remove this hook and use useLayerQueryables instead
export const useLayerKeys = (layerId: string) => {
  const { data, isLoading, error } = useSWR<LayerPaginated>(
    [`${COLLECTIONS_API_BASE_URL}/${layerId}/queryables`],
    fetcher
  );
  return { data, isLoading, error };
};

export const useLayerClassBreaks = (
  layerId: string,
  operation?: ClassBreaks,
  column?: string,
  breaks?: number
) => {
  const { data, isLoading, error } = useSWR<LayerClassBreaks>(
    () =>
      operation && column && breaks
        ? [
            `${PROCESSES_API_BASE_URL}/class-breaks/execution`,
            {
              inputs: {
                collection: layerId,
                attribute: column,
                method: operation,
                breaks: breaks,
              },
            },
          ]
        : null,
    processExecuteFetcher
  );
  return { classBreaks: data, isLoading, isError: error };
};

export const deleteLayer = async (id: string): Promise<Job> => {
  // user_id is extracted from JWT token by the server
  return executeProcessAsync("layer_delete", {
    layer_id: id,
  });
};

/**
 * Create a new layer from a dataset using OGC API Processes (LayerImport).
 * Supports both S3 file uploads and WFS imports.
 * Layer type (feature or table) is auto-detected based on geometry presence.
 * user_id is extracted from JWT token by the server.
 */
export const createLayer = async (
  payload: CreateLayerFromDataset & {
    // Optional WFS import fields
    url?: string;
    other_properties?: Record<string, unknown>;
  },
  projectId?: string
): Promise<Job> => {
  // Map to LayerImport process inputs
  // user_id is extracted from JWT token by the server
  const inputs: Record<string, unknown> = {
    layer_id: crypto.randomUUID(), // Generate new layer ID
    folder_id: payload.folder_id,
    name: payload.name,
    ...(payload.description && { description: payload.description }),
    ...(payload.tags && { tags: payload.tags }),
    ...(projectId && { project_id: projectId }),
    // S3 upload path
    ...(payload.s3_key && { s3_key: payload.s3_key }),
    // WFS import path
    ...(payload.url && { wfs_url: payload.url }),
    ...(payload.other_properties && { other_properties: payload.other_properties }),
    ...(payload.has_header !== undefined && { has_header: payload.has_header }),
    ...(payload.sheet_name && { sheet_name: payload.sheet_name }),
  };

  return executeProcessAsync("layer_import", inputs);
};

/**
 * Create a new raster/external layer (WMS, WMTS, XYZ, COG).
 * These don't upload data, just reference external URLs.
 */
export const createRasterLayer = async (payload: CreateRasterLayer, projectId?: string) => {
  const url = new URL(`${LAYERS_API_BASE_URL}/raster`);
  if (projectId) {
    url.searchParams.append("project_id", projectId);
  }
  const response = await apiRequestAuth(url.toString(), {
    method: "POST",
    body: JSON.stringify(payload),
    headers: {
      "Content-Type": "application/json",
    },
  });
  if (!response.ok) {
    throw new Error("Failed to create raster layer");
  }
  return await response.json();
};

/**
 * Create a new empty layer with user-defined fields.
 * Executed as a Windmill job via the Processes API.
 */
export const createEmptyLayer = async (
  payload: {
    name: string;
    geometry_type: "point" | "line" | "polygon" | null;
    fields: Array<{ name: string; type: "string" | "number" }>;
  },
  projectId: string
): Promise<Job> => {
  const inputs: Record<string, unknown> = {
    name: payload.name,
    geometry_type: payload.geometry_type,
    fields: payload.fields,
    project_id: projectId,
  };
  return executeProcessAsync("layer_create", inputs);
};

export const getLayerClassBreaks = async (
  layerId: string,
  operation: ClassBreaks,
  column: string,
  breaks: number
): Promise<LayerClassBreaks> => {
  const response = await apiRequestAuth(`${PROCESSES_API_BASE_URL}/class-breaks/execution`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      inputs: {
        collection: layerId,
        attribute: column,
        method: operation,
        breaks: breaks,
      },
    }),
  });
  if (!response.ok) {
    throw new Error("Failed to get class breaks");
  }
  return await response.json();
};

export const getLayerUniqueValues = async (
  layerId: string,
  column: string,
  size?: number
): Promise<LayerUniqueValuesPaginated> => {
  const response = await apiRequestAuth(`${PROCESSES_API_BASE_URL}/unique-values/execution`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      inputs: {
        collection: layerId,
        attribute: column,
        limit: size || 100,
      },
    }),
  });
  if (!response.ok) {
    throw new Error("Failed to get unique values");
  }
  // Transform OGC Processes response to legacy paginated format
  const result = await response.json();
  return {
    items: result.values.map((v: { value: string | number; count: number }) => ({
      value: String(v.value),
      count: v.count,
    })),
    total: result.total,
    page: 1,
    size: result.values.length,
    pages: 1,
  };
};

/**
 * Transform OGC Processes unique-values response to legacy paginated format
 */
const transformUniqueValuesResponse = (result: {
  values: { value: string | number | null; count: number }[];
  total: number;
}): LayerUniqueValuesPaginated => ({
  items: result.values.map((v) => ({
    value: String(v.value ?? ""),
    count: v.count,
  })),
  total: result.total,
  page: 1,
  size: result.values.length,
  pages: 1,
});

export const useUniqueValues = (layerId: string, column: string, page?: number) => {
  const offset = page ? (page - 1) * 50 : 0;
  const { data, isLoading, error } = useSWR<LayerUniqueValuesPaginated>(
    layerId && column
      ? [
          `${PROCESSES_API_BASE_URL}/unique-values/execution`,
          {
            inputs: {
              collection: layerId,
              attribute: column,
              limit: 50,
              offset: offset,
            },
          },
        ]
      : null,
    async ([url, body]: [string, object]) => {
      const response = await apiRequestAuth(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!response.ok) throw new Error("Failed to get unique values");
      const result = await response.json();
      return transformUniqueValuesResponse(result);
    }
  );
  return { data, isLoading, error };
};

export const useLayerUniqueValues = (
  layerId: string,
  column: string,
  queryParams?: GetLayerUniqueValuesQueryParams
) => {
  const limit = queryParams?.size || 50;
  const offset = queryParams?.page ? (queryParams.page - 1) * limit : 0;
  const { data, isLoading, error, mutate, isValidating } = useSWR<LayerUniqueValuesPaginated>(
    layerId && column
      ? [
          `${PROCESSES_API_BASE_URL}/unique-values/execution`,
          {
            inputs: {
              collection: layerId,
              attribute: column,
              order: queryParams?.order || "descendent",
              limit: limit,
              offset: offset,
              ...(queryParams?.query && { filter: queryParams.query }),
            },
          },
        ]
      : null,
    async ([url, body]: [string, object]) => {
      const response = await apiRequestAuth(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!response.ok) throw new Error("Failed to get unique values");
      const result = await response.json();
      const transformed = transformUniqueValuesResponse(result);
      // Add proper pagination info
      transformed.page = queryParams?.page || 1;
      transformed.size = limit;
      transformed.pages = Math.ceil(transformed.total / limit);
      return transformed;
    }
  );
  return { data, isLoading, error, mutate, isValidating };
};

/**
 * Start a dataset export job using OGC API Processes.
 * Returns a Job that can be polled for completion.
 * When job is finished, use getExportDownloadUrl to get the download URL.
 */
export const startDatasetExport = async (
  payload: DatasetDownloadRequest & { user_id: string; layer_owner_id: string }
): Promise<Job> => {
  const inputs = {
    user_id: payload.user_id,
    layer_id: payload.id,
    layer_owner_id: payload.layer_owner_id,
    file_type: payload.file_type,
    file_name: payload.file_name,
    ...(payload.crs && { crs: payload.crs }),
    ...(payload.query && { query: payload.query }),
  };

  return executeProcessAsync("layer_export", inputs);
};

/**
 * Download a public layer directly from GeoAPI (no auth required).
 * Fetches the file as a blob and triggers a browser download.
 */
export const downloadLayerDirect = async (
  layerId: string,
  format: string,
  fileName: string,
  crs?: string
): Promise<void> => {
  const params = new URLSearchParams({ format });
  if (crs) {
    params.set("crs", crs);
  }

  const url = `${COLLECTIONS_API_BASE_URL}/${layerId}/download?${params.toString()}`;
  const response = await fetch(url);

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: "Download failed" }));
    throw new Error(error.detail || `Download failed: ${response.status}`);
  }

  const blob = await response.blob();
  const downloadUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = downloadUrl;
  link.download = `${fileName}.zip`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(downloadUrl);
};

export const useClassBreak = (layerId: string, operation: string, column: string, breaks: number) => {
  const { data, isLoading, error } = useSWR<LayerClassBreaks>(
    layerId && operation && column && breaks
      ? [
          `${PROCESSES_API_BASE_URL}/class-breaks/execution`,
          {
            inputs: {
              collection: layerId,
              attribute: column,
              method: operation,
              breaks: breaks,
            },
          },
        ]
      : null,
    processExecuteFetcher
  );
  return { data, isLoading, error };
};

// --- Feature Write API Functions ---

export const getFeature = async (layerId: string, featureId: string) => {
  const response = await apiRequestAuth(`${COLLECTIONS_API_BASE_URL}/${layerId}/items/${featureId}`, {
    method: "GET",
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to get feature");
  }
  return response.json() as Promise<GeoJSON.Feature>;
};

/**
 * Fetch features from a collection with optional query parameters.
 */
export const getFeatures = async (
  layerId: string,
  params?: {
    filter?: Record<string, unknown>;
    limit?: number;
    offset?: number;
    properties?: string[];
  },
): Promise<GeoJSON.FeatureCollection> => {
  const parts: string[] = [];
  if (params?.filter) {
    parts.push(`filter=${encodeURIComponent(JSON.stringify(params.filter))}`);
    parts.push("filter-lang=cql2-json");
  }
  if (params?.limit) parts.push(`limit=${params.limit}`);
  if (params?.offset) parts.push(`offset=${params.offset}`);
  if (params?.properties) parts.push(`properties=${params.properties.join(",")}`);
  const query = parts.length > 0 ? `?${parts.join("&")}` : "";
  const response = await apiRequestAuth(
    `${COLLECTIONS_API_BASE_URL}/${layerId}/items${query}`,
    { method: "GET" },
  );
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to get features");
  }
  return response.json() as Promise<GeoJSON.FeatureCollection>;
};

export const createFeature = async (
  layerId: string,
  feature: { geometry?: Record<string, unknown> | null; properties: Record<string, unknown> }
) => {
  const response = await apiRequestAuth(`${COLLECTIONS_API_BASE_URL}/${layerId}/items`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: "Feature",
      geometry: feature.geometry || null,
      properties: feature.properties,
    }),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to create feature");
  }
  return response.json();
};

export const createFeaturesBulk = async (
  layerId: string,
  features: Array<{ geometry?: Record<string, unknown> | null; properties: Record<string, unknown> }>
) => {
  const response = await apiRequestAuth(`${COLLECTIONS_API_BASE_URL}/${layerId}/items`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: "FeatureCollection",
      features: features.map((f) => ({
        type: "Feature",
        geometry: f.geometry || null,
        properties: f.properties,
      })),
    }),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to create features");
  }
  return response.json();
};

export const updateFeatureProperties = async (
  layerId: string,
  featureId: string,
  properties: Record<string, unknown>
) => {
  const response = await apiRequestAuth(`${COLLECTIONS_API_BASE_URL}/${layerId}/items/${featureId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ properties }),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to update feature");
  }
  return response.json();
};

export const replaceFeature = async (
  layerId: string,
  featureId: string,
  feature: { geometry?: Record<string, unknown> | null; properties: Record<string, unknown> }
) => {
  const response = await apiRequestAuth(`${COLLECTIONS_API_BASE_URL}/${layerId}/items/${featureId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: "Feature",
      geometry: feature.geometry || null,
      properties: feature.properties,
    }),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to replace feature");
  }
  return response.json();
};

export const deleteFeature = async (layerId: string, featureId: string) => {
  const response = await apiRequestAuth(`${COLLECTIONS_API_BASE_URL}/${layerId}/items/${featureId}`, {
    method: "DELETE",
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to delete feature");
  }
  return response.json();
};

export const deleteFeaturesBulk = async (layerId: string, featureIds: string[]) => {
  const response = await apiRequestAuth(`${COLLECTIONS_API_BASE_URL}/${layerId}/items/delete`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ids: featureIds }),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to delete features");
  }
  return response.json();
};

// --- Column Management API Functions ---
//
// These hit geoapi, which manages both the DuckLake DDL and the
// `customer.layer.field_config` JSONB metadata in one place.

export interface AddColumnPayload {
  name: string;
  kind: FieldKind;
  display_config?: Record<string, unknown>;
  default_value?: unknown;
}

export const addColumn = async (layerId: string, payload: AddColumnPayload) => {
  const response = await apiRequestAuth(
    `${COLLECTIONS_API_BASE_URL}/${layerId}/columns`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }
  );
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to add column");
  }
  return response.json();
};

export const renameColumn = async (
  layerId: string,
  columnName: string,
  newName: string,
) => {
  const response = await apiRequestAuth(
    `${COLLECTIONS_API_BASE_URL}/${layerId}/columns/${columnName}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ new_name: newName }),
    }
  );
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to rename column");
  }
  return response.json();
};

export const updateColumnDisplayConfig = async (
  layerId: string,
  columnName: string,
  displayConfig: Record<string, unknown>,
) => {
  const response = await apiRequestAuth(
    `${COLLECTIONS_API_BASE_URL}/${layerId}/columns/${columnName}`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ display_config: displayConfig }),
    }
  );
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to update column");
  }
  return response.json();
};

export const deleteColumn = async (layerId: string, columnName: string) => {
  const response = await apiRequestAuth(
    `${COLLECTIONS_API_BASE_URL}/${layerId}/columns/${columnName}`,
    { method: "DELETE" } 
  );
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || "Failed to delete column");
  }
  return null; // 204 No Content
};
