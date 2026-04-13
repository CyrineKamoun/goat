export type FeatureEditMode = "select" | "draw";

export type PendingFeatureAction = "create" | "update" | "delete";

export interface PendingFeature {
  /** Client-generated UUID for new features, or original feature ID for edits */
  id: string;
  /** MapboxDraw feature ID (while being edited on map, null after committed) */
  drawFeatureId: string | null;
  /** GeoJSON geometry (null until drawn) */
  geometry: GeoJSON.Geometry | null;
  /** User-editable properties */
  properties: Record<string, unknown>;
  /** Whether this feature has been committed via "Done" */
  committed: boolean;
  /** Whether this is a new feature or an edit of an existing one */
  action: PendingFeatureAction;
  /** Original geometry when loaded for editing (for change detection) */
  originalGeometry?: GeoJSON.Geometry | null;
  /** Original properties when loaded for editing (for change detection) */
  originalProperties?: Record<string, unknown>;
}

export interface FeatureEditorState {
  /** Layer currently being edited (layer_id UUID) */
  activeLayerId: string | null;
  /** Geometry type of the active layer */
  geometryType: "point" | "line" | "polygon" | null;
  /** Current tool mode */
  mode: FeatureEditMode;
  /** Map of pending feature ID -> PendingFeature */
  pendingFeatures: Record<string, PendingFeature>;
  /** ID of the feature currently being drawn/edited in the panel */
  activeFeatureId: string | null;
  /** Whether a save operation is in progress */
  isSaving: boolean;
  /** Undo stack — snapshots of pendingFeatures before each recordable action */
  undoStack: EditHistoryEntry[];
  /** Redo stack — snapshots for redo */
  redoStack: EditHistoryEntry[];
}

export interface EditHistoryEntry {
  pendingFeatures: Record<string, PendingFeature>;
  activeFeatureId: string | null;
  mode: FeatureEditMode;
  /** Serialized MapboxDraw features — restored via drawControl.set() */
  drawFeatures: GeoJSON.FeatureCollection;
}
