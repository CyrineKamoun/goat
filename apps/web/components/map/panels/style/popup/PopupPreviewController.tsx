import { useEffect } from "react";

import { useSampleFeature } from "@/components/map/popover/sampleFeature";

import { setPopupPreview } from "@/lib/store/map/slice";

import { useAppDispatch } from "@/hooks/store/ContextHooks";

interface Props {
  layerId: string;
  enabled: boolean;
}

/**
 * Mounts while the Popup section "Show preview" toggle is on. Fetches a sample
 * feature for the layer via `useSampleFeature` and dispatches it to the map
 * slice so `MapViewer` can pin a real `MapFeaturePopover` to it. Clears the
 * preview when the controller unmounts or `enabled` flips false.
 */
export function PopupPreviewController({ layerId, enabled }: Props) {
  const dispatch = useAppDispatch();
  const { feature } = useSampleFeature(layerId, enabled);

  useEffect(() => {
    if (enabled && feature) {
      dispatch(
        setPopupPreview({
          layerId,
          feature,
        }),
      );
    }
    return () => {
      dispatch(setPopupPreview(null));
    };
  }, [enabled, feature, layerId, dispatch]);

  return null;
}
