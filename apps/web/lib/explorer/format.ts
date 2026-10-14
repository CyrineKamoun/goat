/** How the Explorer writes its figures: times as on a timetable, money and
 * counts in the reader's language, estimates rounded. */

/** Seconds since the start of the service day as `HH:MM`; past midnight keeps
 * counting (`24:48`), as a timetable does. */
export function clock(seconds: number | null | undefined): string {
  if (seconds == null) return "–";
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
}

export function count(value: number, locale: string, digits = 0): string {
  return new Intl.NumberFormat(locale, { maximumFractionDigits: digits }).format(value);
}

/** A large figure in a few digits, `54.7M`: totals a year are estimates. */
export function compact(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, { notation: "compact", maximumSignificantDigits: 3 }).format(value);
}

/** An estimate in euros, rounded to what it can claim: `€1.58M`, `€40k`. */
export function euros(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency: "EUR",
    notation: "compact",
    maximumSignificantDigits: 3,
  }).format(value);
}

export function rate(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, {
    style: "currency",
    currency: "EUR",
    minimumFractionDigits: 2,
  }).format(value);
}

/** `Tue, 6 Oct`. */
export function shortDate(date: string, locale: string): string {
  return new Intl.DateTimeFormat(locale, {
    weekday: "short",
    day: "numeric",
    month: "short",
    timeZone: "UTC",
  }).format(new Date(`${date}T00:00:00Z`));
}
