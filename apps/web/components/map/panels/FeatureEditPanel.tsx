import { Button, Stack, Typography } from "@mui/material";
import { useEffect, useRef } from "react";
import { useTranslation } from "react-i18next";

import {
  addPendingFeature,
  commitFeature,
  markForDeletion,
  pushSnapshot,
  setMode,
  removePendingFeature,
  updatePendingGeometry,
  updatePendingProperties,
} from "@/lib/store/featureEditor/slice";
import { useDraw } from "@/lib/providers/DrawProvider";
import { useAppDispatch, useAppSelector } from "@/hooks/store/ContextHooks";
import useLayerFields from "@/hooks/map/CommonHooks";

import Container from "@/components/map/panels/Container";
import TextFieldInput from "@/components/map/panels/common/TextFieldInput";

const FeatureEditPanel: React.FC = () => {
  const { t } = useTranslation("common");
  const dispatch = useAppDispatch();
  const { drawControl } = useDraw();
  const { activeLayerId, activeFeatureId, pendingFeatures, mode, geometryType } = useAppSelector(
    (state) => state.featureEditor
  );
  const isTableLayer = !geometryType;
  const { layerFields } = useLayerFields(activeLayerId || "");

  const pushHistory = () => {
    const drawFeatures = drawControl?.getAll() || { type: "FeatureCollection" as const, features: [] };
    dispatch(pushSnapshot({ drawFeatures }));
  };

  const filteredFields = layerFields.filter(
    (f) => f.type === "string" || f.type === "number"
  );

  // In draw mode, eagerly create a pending feature so user can fill attributes before drawing
  const createdForDrawRef = useRef(false);
  useEffect(() => {
    if (mode === "draw" && activeLayerId && !activeFeatureId && !createdForDrawRef.current) {
      createdForDrawRef.current = true;
      dispatch(
        addPendingFeature({
          id: crypto.randomUUID(),
          drawFeatureId: null,
          geometry: null,
          properties: {},
          committed: false,
          action: "create",
        })
      );
    }
    if (mode !== "draw") {
      createdForDrawRef.current = false;
    }
  }, [mode, activeLayerId, activeFeatureId, dispatch]);

  // Track which fields have had a snapshot pushed (reset on feature change)
  const snapshotPushedFieldsRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    snapshotPushedFieldsRef.current.clear();
  }, [activeFeatureId]);

  // Show panel in draw mode or when a feature is selected
  if (!activeLayerId) return null;
  if (mode === "select" && !activeFeatureId) return null;

  const feature = activeFeatureId ? pendingFeatures[activeFeatureId] : null;
  const hasGeometry = feature?.geometry !== null && feature?.geometry !== undefined;

  // For update features, check if anything actually changed
  const hasChanges = (() => {
    if (!feature) return false;
    // For table layers, new features are always committable (no geometry needed)
    // For geospatial layers, require geometry to be drawn
    if (feature.action === "create") return isTableLayer || hasGeometry;
    // Compare geometry and properties with originals
    const geomChanged = JSON.stringify(feature.geometry) !== JSON.stringify(feature.originalGeometry);
    const filterInternal = (props: Record<string, unknown>) => {
      const f = { ...props }; delete f._fillColor; delete f._fillOpacity; return f;
    };
    const propsChanged = JSON.stringify(filterInternal(feature.properties)) !== JSON.stringify(filterInternal(feature.originalProperties || {}));
    return geomChanged || propsChanged;
  })();

  const handlePropertyChange = (fieldName: string, value: string) => {
    if (!activeFeatureId || !feature) return;

    // Push snapshot on first change per field
    if (!snapshotPushedFieldsRef.current.has(fieldName)) {
      snapshotPushedFieldsRef.current.add(fieldName);
      pushHistory();
    }

    const parsedValue = filteredFields.find((f) => f.name === fieldName)?.type === "number"
      ? (value === "" ? null : Number(value))
      : (value || null);
    dispatch(
      updatePendingProperties({
        id: activeFeatureId,
        properties: { ...feature.properties, [fieldName]: parsedValue },
      })
    );
  };

  const handleDone = () => {
    if (!activeFeatureId || !feature) return;
    // Remove from MapboxDraw — committed features render via GeoJSON overlay
    if (feature.drawFeatureId && drawControl) {
      const drawFeature = drawControl.get(feature.drawFeatureId);
      if (drawFeature?.geometry) {
        dispatch(updatePendingGeometry({ id: activeFeatureId, geometry: drawFeature.geometry }));
      }
      drawControl.delete(feature.drawFeatureId);
    }
    pushHistory();
    dispatch(commitFeature(activeFeatureId));
    // Return to draw mode only if this was a new feature, otherwise stay in select
    if (feature.action === "create") {
      dispatch(setMode("draw"));
    }
  };

  const handleCancel = () => {
    if (activeFeatureId) {
      if (feature?.drawFeatureId && drawControl) {
        drawControl.delete(feature.drawFeatureId);
      }
      dispatch(removePendingFeature(activeFeatureId));
    }
    dispatch(setMode("select"));
  };

  const handleDelete = () => {
    if (!activeFeatureId || !feature) return;
    if (feature.drawFeatureId && drawControl) {
      drawControl.delete(feature.drawFeatureId);
    }
    pushHistory();
    if (feature.action === "update") {
      dispatch(markForDeletion(activeFeatureId));
    } else {
      dispatch(removePendingFeature(activeFeatureId));
    }
  };

  return (
    <Container
      title={t("feature_attributes")}
      close={handleCancel}
      body={
        <Stack spacing={2}>
          {filteredFields.map((field) => (
            <TextFieldInput
              key={field.name}
              label={field.name}
              type={field.type === "number" ? "number" : "text"}
              placeholder={field.type === "number" ? t("enter_a_number") : t("enter_text")}
              value={feature?.properties[field.name] != null ? String(feature.properties[field.name]) : ""}
              onChange={(value) => handlePropertyChange(field.name, value)}
              clearable={false}
            />
          ))}
        </Stack>
      }
      action={
        <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ width: "100%" }}>
          {feature?.committed || feature?.action === "update" ? (
            <Button
              variant="text"
              size="small"
              color="error"
              onClick={handleDelete}
              sx={{ textTransform: "none" }}>
              {t("delete")}
            </Button>
          ) : (
            <Typography variant="caption" color="text.secondary">
              {!hasGeometry ? t("draw_geometry_first") : ""}
            </Typography>
          )}
          <Button
            variant="contained"
            size="small"
            disabled={!hasChanges}
            onClick={handleDone}
            sx={{ textTransform: "none", minWidth: 80 }}>
            {t("done")}
          </Button>
        </Stack>
      }
    />
  );
};

export default FeatureEditPanel;
