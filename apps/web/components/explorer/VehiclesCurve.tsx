/** Vehicles in service through the day, every 5 minutes (board 6). */
import { useTheme } from "@mui/material";
import { Area, AreaChart, ReferenceLine, ResponsiveContainer, XAxis, YAxis } from "recharts";

const STEP_S = 300;

export function VehiclesCurve({
  values,
  color,
  markIndex,
}: {
  values: number[];
  color: string;
  markIndex: number | null;
}) {
  const theme = useTheme();
  const data = values.map((v, i) => ({ t: i * STEP_S, v }));
  const ticks = [6, 9, 12, 15, 18, 21, 24].map((h) => h * 3600);
  return (
    <ResponsiveContainer width="100%" height={80}>
      <AreaChart data={data} margin={{ top: 6, right: 4, bottom: 0, left: 4 }}>
        <XAxis
          dataKey="t"
          type="number"
          domain={[5 * 3600, Math.max(25 * 3600, data.length * STEP_S)]}
          ticks={ticks}
          tickFormatter={(s: number) => `${String(Math.floor(s / 3600) % 24).padStart(2, "0")}:00`}
          tick={{ fontSize: 10, fill: theme.palette.text.secondary }}
          axisLine={false}
          tickLine={false}
        />
        <YAxis hide domain={[0, "dataMax"]} />
        <Area
          type="stepAfter"
          dataKey="v"
          stroke={color}
          fill={color}
          fillOpacity={0.15}
          isAnimationActive={false}
        />
        {markIndex !== null && (
          <ReferenceLine x={markIndex * STEP_S} stroke={theme.palette.text.secondary} strokeDasharray="3 3" />
        )}
      </AreaChart>
    </ResponsiveContainer>
  );
}
