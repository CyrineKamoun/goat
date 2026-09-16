import { useSearchParams } from "next/navigation";

export interface AnnouncementPreview {
  /** `?preview=welcome` */
  welcome: boolean;
  /** `?preview=spotlight` → `"*"` (the newest spotlight); `?preview=spotlight:<id>` → that entry's id. */
  spotlight: string | null;
}

/**
 * `?preview=…` on Home reopens an announcement regardless of stage or history
 * and records nothing, so a dismissed Welcome or spotlight can be looked at
 * again — by a user who wants to, or by support reproducing what a user saw.
 * Works in every environment on purpose: there is nothing to protect, since
 * the preview only shows content that is already public.
 */
export const useAnnouncementPreview = (): AnnouncementPreview => {
  const preview = useSearchParams()?.get("preview") ?? null;
  return {
    welcome: preview === "welcome",
    spotlight:
      preview === "spotlight"
        ? "*"
        : preview?.startsWith("spotlight:")
          ? preview.slice("spotlight:".length)
          : null,
  };
};
