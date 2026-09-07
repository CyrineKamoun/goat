/**
 * The one way a template snapshot turns into pixels: an SVG string drawn
 * into a canvas through an `<img>`, which is what rasterises it without a
 * rendering library. Both snapshots — the workflow's canvas and the layout's
 * wireframe — go through here, so they are produced the same way and fail
 * the same way.
 *
 * The SVG travels as a data URL rather than a blob URL, so nothing has to be
 * revoked and the canvas is never tainted. Every failure — a browser that
 * refuses the image, no 2D context, a canvas that hands back no blob —
 * rejects, and the caller falls back to the drawn scaffold.
 */
export const rasterizeSvg = (
  svg: string,
  size: { width: number; height: number },
  background: string
): Promise<Blob> =>
  new Promise((resolve, reject) => {
    const image = new Image();
    image.onload = () => {
      try {
        const canvas = document.createElement("canvas");
        canvas.width = size.width;
        canvas.height = size.height;
        const context = canvas.getContext("2d");
        if (!context) {
          reject(new Error("Canvas 2D context unavailable"));
          return;
        }
        context.fillStyle = background;
        context.fillRect(0, 0, size.width, size.height);
        context.drawImage(image, 0, 0, size.width, size.height);
        canvas.toBlob((blob) => {
          if (blob) resolve(blob);
          else reject(new Error("Canvas produced no PNG"));
        }, "image/png");
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
    image.onerror = () => reject(new Error("Snapshot image failed to load"));
    image.src = `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svg)}`;
  });
