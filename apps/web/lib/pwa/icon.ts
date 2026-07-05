import sharp from "sharp";

export const PWA_ICON_SIZES = [180, 192, 512] as const;
export type PwaIconSize = (typeof PWA_ICON_SIZES)[number];

export function isAllowedIconSize(size: number): size is PwaIconSize {
  return (PWA_ICON_SIZES as readonly number[]).includes(size);
}

// The favicon is deliberately not a fallback here — small favicons make
// bad home-screen icons. null means: use the GOAT logo.
export function resolveAppIconUrl(
  settings: { app_icon_url?: string | null } | null | undefined
): string | null {
  const url = settings?.app_icon_url;
  return url ? url : null;
}

export async function rasterizeToPng(source: Buffer, size: number): Promise<Buffer> {
  // density only affects vector input (SVG); it makes sharp render the
  // SVG large enough that the resize is a downscale, not a blurry upscale.
  return sharp(source, { density: 300 })
    .resize(size, size, {
      fit: "contain",
      background: { r: 0, g: 0, b: 0, alpha: 0 },
    })
    .png()
    .toBuffer();
}
