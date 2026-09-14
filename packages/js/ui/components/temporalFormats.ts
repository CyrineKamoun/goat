/** Wire format for temporal values: naive ISO wall time, interpreted as UTC. */
export const TEMPORAL_VALUE_FORMAT = "YYYY-MM-DDTHH:mm:ss";

/** Wire format for date-only values. */
export const TEMPORAL_DATE_FORMAT = "YYYY-MM-DD";

/** Display format for a datetime rendered as stored data — a table cell, a
 * popup row. Read next to other cells rather than on its own, so it stays the
 * sortable ISO ordering and needs no locale to produce. */
export const TEMPORAL_DISPLAY_FORMAT = "YYYY-MM-DD HH:mm";

// A picker is the other case: a single date read and typed by one person, where
// the ordering they expect is their own. `L` is dayjs's localized date token,
// so the separators and field order come from the loaded locale — `14.09.2026`
// in German, `14/09/2026` in English — while the value emitted stays ISO.

/** Display format for a date-only picker. */
export const TEMPORAL_DATE_PICKER_FORMAT = "L";

/** Display format for a datetime picker (24h). */
export const TEMPORAL_DATETIME_PICKER_FORMAT = "L HH:mm";

/** Locales the pickers can render in, keyed as dayjs names. Day-first in both:
 * `en-gb` rather than `en`, matching the date-fns locale the app already
 * picks, so English does not switch to month-first ordering. */
export type TemporalLocale = "de" | "en-gb";
