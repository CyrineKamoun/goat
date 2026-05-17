import type { MapRef } from "react-map-gl/maplibre";

type RGB = { r: number; g: number; b: number };

const DEFAULT_BLUE: RGB = { r: 70, g: 130, b: 180 };

export type CreatePulsingDotOptions = {
  /**
   * CSS color string (e.g. "#rrggbb", "rgb(r,g,b)") used for both the
   * inner dot fill and the outer expanding ring. Falls back to the
   * legacy user-location blue when omitted or unparseable.
   */
  color?: string;
  size?: number;
  duration?: number;
  idleTime?: number;
  /**
   * Whether to draw a white stroke around the inner dot. Defaults to
   * true (matches legacy user-location dot). The popup-active variant
   * disables it for a crisper edge against a dark basemap.
   */
  innerBorder?: boolean;
  /**
   * Inner static dot radius as a fraction of half the canvas size.
   * Defaults to 0.3 (legacy). Lower values keep the inner dot small
   * while leaving more room for the outer ring to expand — useful
   * when bumping `size` to grow the blast radius.
   */
  innerRatio?: number;
  /**
   * Skip the expanding outer ring entirely — render just the static
   * inner dot. Also skips `triggerRepaint`, so it costs nothing per
   * frame. Used for the hover-triggered popup highlight where the
   * pulse animation would be too attention-grabbing.
   */
  staticOnly?: boolean;
};

function parseCssColor(input: string): RGB | null {
  const value = input.trim();
  // #rgb or #rrggbb
  const hexMatch = /^#?([0-9a-f]{3}|[0-9a-f]{6})$/i.exec(value);
  if (hexMatch) {
    const hex = hexMatch[1];
    if (hex.length === 3) {
      return {
        r: parseInt(hex[0] + hex[0], 16),
        g: parseInt(hex[1] + hex[1], 16),
        b: parseInt(hex[2] + hex[2], 16),
      };
    }
    return {
      r: parseInt(hex.slice(0, 2), 16),
      g: parseInt(hex.slice(2, 4), 16),
      b: parseInt(hex.slice(4, 6), 16),
    };
  }
  // rgb(r,g,b) / rgba(r,g,b,a)
  const rgbMatch = /^rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/i.exec(value);
  if (rgbMatch) {
    return {
      r: Number(rgbMatch[1]),
      g: Number(rgbMatch[2]),
      b: Number(rgbMatch[3]),
    };
  }
  return null;
}

function createPulsingDot(
  map: MapRef,
  sizeOrOptions: number | CreatePulsingDotOptions = 85,
  innerColor: RGB = DEFAULT_BLUE,
  outerColor: RGB = DEFAULT_BLUE,
  duration: number = 800, // Expansion phase duration
  idleTime: number = 3000 // Pause duration (3000ms or 3 seconds)
) {
  // Support new options-object signature while preserving the legacy
  // positional one. When called as `createPulsingDot(map, { color })`,
  // a single CSS color tints both the inner fill and the outer ring.
  let size: number;
  let innerBorder = true;
  let innerRatio = 0.3;
  let staticOnly = false;
  if (typeof sizeOrOptions === "object" && sizeOrOptions !== null) {
    size = sizeOrOptions.size ?? 85;
    duration = sizeOrOptions.duration ?? duration;
    idleTime = sizeOrOptions.idleTime ?? idleTime;
    if (sizeOrOptions.innerBorder === false) innerBorder = false;
    if (typeof sizeOrOptions.innerRatio === "number") innerRatio = sizeOrOptions.innerRatio;
    if (sizeOrOptions.staticOnly === true) staticOnly = true;
    if (sizeOrOptions.color) {
      const parsed = parseCssColor(sizeOrOptions.color);
      if (parsed) {
        innerColor = parsed;
        outerColor = parsed;
      }
    }
  } else {
    size = sizeOrOptions;
  }
  const totalDuration = duration + idleTime; // Total cycle duration.

  return {
    width: size,
    height: size,
    data: new Uint8Array(size * size * 4),

    onAdd: function (): void {
      const canvas = document.createElement("canvas");
      canvas.width = this.width;
      canvas.height = this.height;
      this.context = canvas.getContext("2d") as CanvasRenderingContext2D;
    },

    render: function (): boolean {
      // Round the static inner radius to an integer so its edge falls
      // on a pixel boundary — fractional radii anti-alias over more
      // pixels and look soft. Outer ring stays fractional for smooth
      // animation (it's moving fast enough that subpixel stepping isn't
      // visible).
      const radius = Math.round((size / 2) * innerRatio);
      const context = this.context;

      // Clear the canvas.
      context.clearRect(0, 0, this.width, this.height);

      // Static variant: paint the inner dot once, skip the pulse loop,
      // skip triggerRepaint (no per-frame cost). MapLibre still reads
      // `this.data` each render but the canvas doesn't re-draw.
      if (staticOnly) {
        context.beginPath();
        context.arc(this.width / 2, this.height / 2, radius, 0, Math.PI * 2);
        context.fillStyle = `rgba(${innerColor.r}, ${innerColor.g}, ${innerColor.b}, 1)`;
        context.fill();
        if (innerBorder) {
          context.strokeStyle = "white";
          context.lineWidth = 2;
          context.stroke();
        }
        this.data = context.getImageData(0, 0, this.width, this.height).data;
        return true;
      }

      const currentTime = performance.now() % totalDuration;

      // Determine if we are in the expansion phase or the idle phase.
      const isExpanding = currentTime <= duration;
      const t = isExpanding ? currentTime / duration : 0; // Normalized time for expansion.

      // Outer ring expands from `radius` out to the canvas edge over
      // the duration phase.
      const outerSpan = size / 2 - radius;
      const outerRadius = isExpanding ? radius + outerSpan * t : 0;

      // Draw the outer circle (stroke only during expansion phase).
      if (isExpanding) {
        context.beginPath();
        context.arc(this.width / 2, this.height / 2, outerRadius, 0, Math.PI * 2);
        context.strokeStyle = `rgba(${outerColor.r}, ${outerColor.g}, ${outerColor.b}, ${1 - t})`;
        context.lineWidth = 4; // Outer circle stroke thickness.
        context.stroke();
      }

      // Draw the inner circle (static).
      context.beginPath();
      context.arc(this.width / 2, this.height / 2, radius, 0, Math.PI * 2);
      context.fillStyle = `rgba(${innerColor.r}, ${innerColor.g}, ${innerColor.b}, 1)`;
      context.fill();
      if (innerBorder) {
        context.strokeStyle = "white";
        context.lineWidth = 2;
        context.stroke();
      }

      // Update this image's data with data from the canvas.
      this.data = context.getImageData(0, 0, this.width, this.height).data;

      // Continuously repaint the map for smooth animation.
      map.triggerRepaint();

      // Return `true` to let the map know that the image was updated.
      return true;
    },
  };
}

export default createPulsingDot;
