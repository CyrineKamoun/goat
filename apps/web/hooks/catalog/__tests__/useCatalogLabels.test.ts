import { renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useCatalogLabels } from "@/hooks/catalog/useCatalogLabels";

/**
 * How a period reads once the data states ranges.
 *
 * `formatPeriod` is the only place the catalog turns two timestamps into one
 * line, and every branch of it is a judgement call about what a person should
 * see — an instant as a date, a multi-year range as years, an open end as words.
 * Worth pinning: the branches differ only on ranged data, which the current
 * harvest barely publishes, so a regression here would look fine on today's
 * corpus.
 */

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // The two open-bound labels, as `en/common.json` defines them.
    t: (key: string, vars?: Record<string, unknown>) =>
      ({
        "common:catalog_period_since": `since ${vars?.year}`,
        "common:catalog_period_until": `until ${vars?.year}`,
      })[key] ?? key,
    i18n: { language: "en-GB", exists: () => false },
  }),
}));

const period = (start?: string, end?: string) =>
  renderHook(() => useCatalogLabels()).result.current.formatPeriod({ start, end });

describe("formatPeriod", () => {
  it("spells the month for a real date, so 6/10 cannot read as 6 October", () => {
    expect(period("2015-06-01T00:00:00Z", "2015-06-01T00:00:00Z")).toBe("1 Jun 2015");
  });

  it("shows the year alone when the value is only a year", () => {
    // 4,689 of 10,793 layers are dated 1 January at midnight — a year with a day
    // bolted on. Printing "1 Jan 2015" states a day the source never claimed.
    expect(period("2015-01-01T00:00:00Z", "2015-01-01T00:00:00Z")).toBe("2015");
  });

  it("shows a span inside one year as the year, not as its first day", () => {
    expect(period("2015-01-01T00:00:00Z", "2015-12-31T00:00:00Z")).toBe("2015");
  });

  it("shows years for a range spanning several", () => {
    expect(period("2014-01-01T00:00:00Z", "2021-12-31T00:00:00Z")).toBe("2014 – 2021");
  });

  it("states an open end rather than dropping it", () => {
    expect(period("2020-01-01T00:00:00Z", undefined)).toBe("since 2020");
    expect(period(undefined, "2020-01-01T00:00:00Z")).toBe("until 2020");
  });

  it("has nothing to show when neither bound is stated", () => {
    expect(period(undefined, undefined)).toBeUndefined();
    expect(
      renderHook(() => useCatalogLabels()).result.current.formatPeriod(undefined)
    ).toBeUndefined();
  });
});

describe("periodField", () => {
  const field = (start?: string, end?: string) =>
    renderHook(() => useCatalogLabels()).result.current.periodField({ start, end });

  it("calls a single date a reference year, and states the year", () => {
    // The heading claims a year, so the value is a year — a labelled row should
    // not be more precise than its label. The card has room to show the day.
    expect(field("2015-06-01T00:00:00Z", "2015-06-01T00:00:00Z")).toEqual({
      labelKey: "common:metadata.headings.data_reference_year",
      value: "2015",
    });
  });

  it("calls a span a period", () => {
    expect(field("2014-01-01T00:00:00Z", "2021-12-31T00:00:00Z")).toEqual({
      labelKey: "common:catalog_datetime",
      value: "2014 – 2021",
    });
  });

  it("follows the value, not the layer count", () => {
    // 3,818 of 3,834 datasets have every layer on one date, bundles included, and
    // 70 single layers carry a range — so binding the heading to the member count
    // would mislabel both ends of the catalog.
    expect(field("2019-01-01T00:00:00Z", "2019-01-01T00:00:00Z")?.labelKey).toBe(
      "common:metadata.headings.data_reference_year"
    );
  });

  it("has nothing to say without a period", () => {
    expect(field(undefined, undefined)).toBeUndefined();
  });
});
