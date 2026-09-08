/**
 * Client-side preview of CSV/XLSX files for the upload wizard.
 * XLSX uses SheetJS (dynamic import to avoid main bundle bloat).
 * CSV uses native FileReader with manual parsing (zero deps).
 */

/**
 * A file as it was read: the head, and how many rows there are.
 *
 * Separate from `TabularPreview` because reading is the expensive half and depends only on
 * the file and the worksheet, while `hasHeader` merely decides how these rows are read.
 * Kept apart, flipping that switch costs nothing — together, it re-read the whole file.
 */
export interface TabularSource {
  sheetNames: string[];
  activeSheet: string;
  /** The first rows as they stand in the file, the header row among them. */
  head: string[][];
  /** Every non-empty row in the file, the header row among them. */
  totalLines: number;
}

export interface TabularPreview {
  sheetNames: string[];
  activeSheet: string;
  headers: string[];
  rows: string[][];
  totalRows: number;
}

/**
 * Twenty, not five: this sample is read in a dialog of its own now, where the point is to
 * see enough of the data to judge whether the header row and the worksheet are right. Five
 * rows was sized for a strip inside a form.
 */
const MAX_PREVIEW_ROWS = 20;

/**
 * How much of a CSV is read to find the preview rows.
 *
 * A slice, not the file: 20 rows of a 6 MB export is a few kilobytes, and reading all of it
 * meant a 6 MB string plus one substring per line — a hundred thousand of them for a narrow
 * file — allocated and thrown away. Wide rows are why this grows rather than being fixed:
 * 400 columns is a few kilobytes per row on its own.
 */
const HEAD_BYTES = 256 * 1024;
const MAX_HEAD_BYTES = 8 * 1024 * 1024;

/**
 * Strip trailing rows where every cell is empty/whitespace.
 * Handles Excel files where cell formatting (e.g. background color)
 * extends the used range far beyond actual data.
 */
function stripTrailingEmptyRows(rows: string[][]): string[][] {
  let lastNonEmpty = rows.length - 1;
  while (lastNonEmpty >= 0) {
    const row = rows[lastNonEmpty];
    if (row.some((cell) => cell.trim() !== "")) break;
    lastNonEmpty--;
  }
  return rows.slice(0, lastNonEmpty + 1);
}

/**
 * Read a tabular file: its worksheets, its head, and its length.
 */
export async function readTabularSource(
  file: File,
  options?: { sheetName?: string }
): Promise<TabularSource> {
  const ext = file.name.split(".").pop()?.toLowerCase();
  if (ext === "xlsx" || ext === "xls") return readXlsxSource(file, options);
  return readCsvSource(file);
}

/**
 * Read the head as either a header row and data, or as data with generated column names.
 *
 * Pure, and cheap: this is what the header switch re-runs.
 */
export function derivePreview(source: TabularSource, hasHeader: boolean): TabularPreview {
  const { sheetNames, activeSheet, head, totalLines } = source;

  if (hasHeader) {
    if (head.length === 0) {
      return { sheetNames, activeSheet, headers: [], rows: [], totalRows: 0 };
    }
    return {
      sheetNames,
      activeSheet,
      headers: head[0].map((value) => String(value)),
      rows: head.slice(1, MAX_PREVIEW_ROWS + 1),
      totalRows: Math.max(0, totalLines - 1),
    };
  }

  const colCount = head.length > 0 ? head[0].length : 0;
  return {
    sheetNames,
    activeSheet,
    headers: Array.from({ length: colCount }, (_, i) => `Column ${i + 1}`),
    rows: head.slice(0, MAX_PREVIEW_ROWS),
    totalRows: totalLines,
  };
}

/** Read a file and derive its preview in one call. */
async function readXlsxSource(
  file: File,
  options?: { sheetName?: string }
): Promise<TabularSource> {
  const XLSX = await import("xlsx");
  const buffer = await file.arrayBuffer();

  // The whole workbook, because SheetJS has no partial read — but once per file and
  // worksheet, not once per header toggle.
  const workbook = XLSX.read(buffer, { type: "array" });
  const sheetNames = workbook.SheetNames;
  const activeSheet = options?.sheetName ?? sheetNames[0];
  const sheet = workbook.Sheets[activeSheet];

  if (!sheet) return { sheetNames, activeSheet, head: [], totalLines: 0 };

  // Strip trailing empty rows caused by cell formatting (e.g. background color)
  // extending beyond actual data.
  const allRows = stripTrailingEmptyRows(
    XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      defval: "",
      rawNumbers: false,
    })
  );

  return {
    sheetNames,
    activeSheet,
    head: allRows.slice(0, MAX_PREVIEW_ROWS + 1).map((r) => r.map((v) => String(v))),
    totalLines: allRows.length,
  };
}

async function readCsvSource(file: File): Promise<TabularSource> {
  const lines = await readHeadLines(file, MAX_PREVIEW_ROWS + 1);
  const delimiter = detectDelimiter(lines[0] ?? "");
  return {
    sheetNames: [],
    activeSheet: "",
    head: lines.map((line) => parseLine(line, delimiter)),
    totalLines: await countLines(file),
  };
}

/**
 * The first `needed` non-empty lines, reading as little of the file as will hold them.
 *
 * The slice grows rather than being one fixed size, because a row's length is unknown until
 * it is seen: 20 rows of a 400-column export do not fit in the first 256 kB.
 */
async function readHeadLines(file: File, needed: number): Promise<string[]> {
  let size = Math.min(HEAD_BYTES, Math.max(file.size, 1));
  for (;;) {
    const atEnd = size >= file.size;
    const parts = (await file.slice(0, size).text()).split(/\r?\n/);
    // The last line of a slice may have been cut in half.
    if (!atEnd) parts.pop();
    const lines = parts.filter((line) => line.trim().length > 0);
    if (lines.length >= needed || atEnd || size >= MAX_HEAD_BYTES) return lines.slice(0, needed);
    size = Math.min(size * 4, file.size, MAX_HEAD_BYTES);
  }
}

/**
 * How many non-empty lines the file has, counted a chunk at a time.
 *
 * One pass over the bytes holding one chunk at a time, where splitting the text held the
 * whole file and an array of every line in it at once. This is only ever a count: the rows
 * themselves come from the head.
 */
async function countLines(file: File): Promise<number> {
  const reader = file.stream().getReader();
  let count = 0;
  let content = false;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    for (let i = 0; i < value.length; i++) {
      const byte = value[i];
      if (byte === 0x0a) {
        if (content) count++;
        content = false;
      } else if (byte > 0x20) {
        content = true;
      }
    }
  }
  // A last line with no newline after it.
  return content ? count + 1 : count;
}

/** One CSV line into fields, honouring quotes and doubled quotes. */
function parseLine(line: string, delimiter: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"' && line[i + 1] === '"') {
        current += '"';
        i++;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        current += ch;
      }
    } else if (ch === delimiter) {
      result.push(current);
      current = "";
    } else if (ch === '"') {
      inQuotes = true;
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

function detectDelimiter(line: string): string {
  const counts: Record<string, number> = { ",": 0, ";": 0, "\t": 0, "|": 0 };
  for (const ch of line) {
    if (ch in counts) counts[ch]++;
  }
  let best = ",";
  let bestCount = 0;
  for (const [delim, count] of Object.entries(counts)) {
    if (count > bestCount) {
      best = delim;
      bestCount = count;
    }
  }
  return best;
}
