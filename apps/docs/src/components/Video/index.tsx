import React, { useEffect, useRef } from "react";
import type { CSSProperties } from "react";

type VideoProps = {
  src: string;
  alt?: string;
  style?: CSSProperties;
};

// Screen recordings are MP4, not GIF: a fraction of the size and in full
// colour. This plays one the way a GIF would (muted, looping, inline), but only
// while it is on screen, and not at all for readers who ask for reduced motion.
// The controls let a reader pause it, or start it where autoplay is blocked
// (iOS Low Power Mode).
export default function Video({ src, alt, style }: VideoProps) {
  const ref = useRef<HTMLVideoElement>(null);

  useEffect(() => {
    const video = ref.current;
    if (!video || window.matchMedia("(prefers-reduced-motion: reduce)").matches) {
      return;
    }
    const observer = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting) {
        video.play().catch(() => {});
      } else {
        video.pause();
      }
    });
    observer.observe(video);
    return () => observer.disconnect();
  }, []);

  return (
    <video
      ref={ref}
      src={src}
      aria-label={alt}
      title={alt}
      style={style}
      muted
      loop
      playsInline
      controls
      preload="metadata"
    />
  );
}
