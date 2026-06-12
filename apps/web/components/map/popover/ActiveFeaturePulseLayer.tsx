import { Layer, Source } from "react-map-gl/maplibre";

interface ActiveFeaturePulseProps {
  lngLat: { lng: number; lat: number } | null;
  visible: boolean;
  /**
   * When false, render the non-pulsing static dot instead of the
   * expanding-ring variant. Used for hover-triggered popups where the
   * pulse would feel too attention-grabbing for a transient highlight.
   * Defaults to true (pulse, matches click-triggered behavior).
   */
  pulse?: boolean;
}

export function ActiveFeaturePulseLayer({
  lngLat,
  visible,
  pulse = true,
}: ActiveFeaturePulseProps) {
  if (!visible || !lngLat) return null;
  const data = {
    type: "FeatureCollection" as const,
    features: [
      {
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [lngLat.lng, lngLat.lat] },
        properties: {},
      },
    ],
  };
  return (
    <Source id="popup-active-feature" type="geojson" data={data}>
      <Layer
        id="popup-active-feature-pulse"
        type="symbol"
        layout={{
          "icon-image": pulse ? "popup-active-pulsing-dot" : "popup-active-static-dot",
          "icon-allow-overlap": true,
        }}
      />
    </Source>
  );
}
