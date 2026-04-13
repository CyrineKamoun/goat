/**
 * Client-side preview of CSV/XLSX files for the upload wizard.
 * XLSX uses SheetJS (dynamic import to avoid main bundle bloat).
 * CSV uses native FileReader with manual parsing (zero deps).
 */

export interface TabularPreview {
  sheetNames: string[];
  activeSheet: string;
  headers: string[];
  rows: string[][];
  totalRows: number;
}

const MAX_PREVIEW_ROWS = 5;

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
 * Parse a tabular file (CSV or XLSX) and return a preview.
 */
export async function parseTabularPreview(
  file: File,
  options?: { sheetName?: string; hasHeader?: boolean }
): Promise<TabularPreview> {
  const ext = file.name.split(".").pop()?.toLowerCase();
  if (ext === "xlsx" || ext === "xls") {
    return parseXlsxPreview(file, options);
  }
  return parseCsvPreview(file, options);
}

async function parseXlsxPreview(
  file: File,
  options?: { sheetName?: string; hasHeader?: boolean }
): Promise<TabularPreview> {
  const XLSX = await import("xlsx");
  const buffer = await file.arrayBuffer();

  // Parse once, then slice for preview. Strip trailing empty rows caused
  // by cell formatting (e.g. background color) extending beyond actual data.
  const workbook = XLSX.read(buffer, { type: "array" });
  const sheetNames = workbook.SheetNames;
  const activeSheet = options?.sheetName ?? sheetNames[0];
  const sheet = workbook.Sheets[activeSheet];

  if (!sheet) {
    return { sheetNames, activeSheet, headers: [], rows: [], totalRows: 0 };
  }

  const allRows = stripTrailingEmptyRows(
    XLSX.utils.sheet_to_json(sheet, {
      header: 1,
      defval: "",
      rawNumbers: false,
    })
  );

  const hasHeader = options?.hasHeader ?? true;
  const totalDataRows = hasHeader ? allRows.length - 1 : allRows.length;

  if (hasHeader && allRows.length > 0) {
    const headers = allRows[0].map((v) => String(v));
    const rows = allRows.slice(1, MAX_PREVIEW_ROWS + 1).map((r) => r.map((v) => String(v)));
    return { sheetNames, activeSheet, headers, rows, totalRows: Math.max(0, totalDataRows) };
  }

  // No header: generate Column 1, Column 2, ...
  const colCount = allRows.length > 0 ? allRows[0].length : 0;
  const headers = Array.from({ length: colCount }, (_, i) => `Column ${i + 1}`);
  const rows = allRows.slice(0, MAX_PREVIEW_ROWS).map((r) => r.map((v) => String(v)));
  return { sheetNames, activeSheet, headers, rows, totalRows: Math.max(0, totalDataRows) };
}

async function parseCsvPreview(
  file: File,
  options?: { sheetName?: string; hasHeader?: boolean }
): Promise<TabularPreview> {
  const text = await file.text();
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0);

  const hasHeader = options?.hasHeader ?? true;
  const delimiter = detectDelimiter(lines[0] ?? "");

  const parseLine = (line: string): string[] => {
    // Simple CSV parsing: handles quoted fields
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
  };

  if (hasHeader && lines.length > 0) {
    const headers = parseLine(lines[0]);
    const rows = lines.slice(1, MAX_PREVIEW_ROWS + 1).map(parseLine);
    return {
      sheetNames: [],
      activeSheet: "",
      headers,
      rows,
      totalRows: lines.length - 1,
    };
  }

  const allParsed = lines.slice(0, MAX_PREVIEW_ROWS).map(parseLine);
  const colCount = allParsed.length > 0 ? allParsed[0].length : 0;
  const headers = Array.from({ length: colCount }, (_, i) => `Column ${i + 1}`);
  return {
    sheetNames: [],
    activeSheet: "",
    headers,
    rows: allParsed,
    totalRows: lines.length,
  };
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
