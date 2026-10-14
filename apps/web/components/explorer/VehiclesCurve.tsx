/**
 * Vehicles in service through the day (board 6). The 5-minute counts flicker
 * where short trips start and end; the chart shows each quarter hour's most,
 * the fleet the quarter needs, from the route's first trip to its last.
 */
import { useTheme } from "@mui/material";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, XAxis, YAxis } from "recharts";

/** The counts' step, and the chart's. */
const STEP_S = 300;
const BUCKET_S = 900;
const HOUR = 3600;

/** Each quarter hour's most vehicles, from 5-minute counts. */
export function quarterHourPeaks(values: number[]): number[] {
  const per = BUCKET_S / STEP_S;
  const out: number[] = [];
  for (let i = 0; i < values.length; i += per) out.push(Math.max(...values.slice(i, i + per)));
  return out;
}

export function VehiclesCurve({ values, color }: { values: number[]; color: string }) {
  const theme = useTheme();
  const peaks = quarterHourPeaks(values);
  const first = peaks.findIndex((v) => v > 0);
  const last = peaks.length - 1 - [...peaks].reverse().findIndex((v) => v > 0);
  if (first < 0) return null;
  // From the first trip to the last: the axis is the route's service day.
  const start = first * BUCKET_S;
  const end = (last + 1) * BUCKET_S;
  const data = peaks.map((v, i) => ({ t: i * BUCKET_S, v })).filter((d) => d.t >= start && d.t <= end);
  // Hour labels as dense as the span allows: every hour of a short day, every
  // 2 or 3 hours of a long one, on the clock's own hours.
  const span = (end - start) / HOUR;
  const every = span <= 5 ? 1 : span <= 10 ? 2 : 3;
  const ticks: number[] = [];
  for (let h = Math.ceil(start / HOUR / every) * every; h * HOUR <= end; h += every) ticks.push(h * HOUR);
  return (
    <ResponsiveContainer width="100%" height={100}>
      <AreaChart data={data} margin={{ top: 6, right: 6, bottom: 0, left: 0 }}>
        <CartesianGrid vertical={false} stroke={theme.palette.divider} strokeDasharray="2 3" />
        <XAxis
          dataKey="t"
          type="number"
          domain={[start, end]}
          ticks={ticks}
          tickFormatter={(s: number) => `${String(Math.floor(s / HOUR) % 24).padStart(2, "0")}:00`}
          tick={{ fontSize: 10, fill: theme.palette.text.secondary }}
          axisLine={{ stroke: theme.palette.divider }}
          tickLine={false}
        />
        {/* Vehicles, in whole ones. */}
        <YAxis
          width={24}
          allowDecimals={false}
          tickCount={4}
          domain={[0, (max: number) => Math.max(1, Math.ceil(max * 1.1))]}
          tick={{ fontSize: 10, fill: theme.palette.text.secondary }}
          axisLine={false}
          tickLine={false}
        />
        <Area
          type="stepAfter"
          dataKey="v"
          stroke={color}
          strokeWidth={1.5}
          fill={color}
          fillOpacity={0.15}
          isAnimationActive={false}
        />
      </AreaChart>
    </ResponsiveContainer>
  );
}
