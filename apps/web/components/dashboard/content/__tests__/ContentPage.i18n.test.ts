import i18next from "i18next";
import { describe, expect, it } from "vitest";

import de from "@/i18n/locales/de/common.json";
import en from "@/i18n/locales/en/common.json";

/**
 * The keys the Content page's toasts render with a count. i18next resolves a
 * counted key through its plural suffixes, so a bare key with no `_one`/
 * `_other` form beside it renders as the raw key in the toast — which is what
 * `template_datasets_need_sharing` did.
 */
const COUNTED_TOAST_KEYS = ["template_datasets_need_sharing"];

const translator = (language: "en" | "de") => {
  const instance = i18next.createInstance();
  instance.init({
    lng: language,
    fallbackLng: false,
    ns: ["common"],
    defaultNS: "common",
    resources: { en: { common: en }, de: { common: de } },
  });
  return instance;
};

describe("Content page toast keys", () => {
  it.each(["en", "de"] as const)("%s resolves every counted toast key", (language) => {
    const t = translator(language);
    for (const key of COUNTED_TOAST_KEYS) {
      for (const count of [1, 3]) {
        const text = t.t(key, { count });
        expect(text).not.toBe(key);
        expect(text.length).toBeGreaterThan(0);
      }
    }
  });

  it("interpolates the count it is given", () => {
    const t = translator("en");
    expect(t.t("template_datasets_need_sharing", { count: 3 })).toContain("3");
  });
});
