import React from "react";
import {interpolate, useCurrentFrame, useVideoConfig} from "remotion";
import {C, F} from "./theme";

export type UnitDot = {
  color?: string;             // default cyan (officers); use theme red for subject
  label?: string;             // "OFFICER 4", "HARRIS"
  startSec?: number;
  mode: "free" | "path";
  /** percent-of-frame coordinates [x%, y%]. free: [anchor]; path: waypoints */
  points: Array<[number, number]>;
  durSec?: number;            // path traversal time (path mode)
  wanderPct?: number;         // free-mode drift radius in % of frame (default 1.2)
  trail?: boolean;            // path mode: draw dashed trail behind the dot
};

const pos = (d: UnitDot, tSec: number): [number, number] => {
  const t = Math.max(0, tSec - (d.startSec ?? 0));
  if (d.mode === "free" || d.points.length < 2) {
    const [ax, ay] = d.points[0];
    const r = d.wanderPct ?? 1.2;
    // smooth organic drift: two incommensurate sines per axis
    const x = ax + r * (Math.sin(t * 0.9) * 0.6 + Math.sin(t * 0.37 + 2.1) * 0.4);
    const y = ay + r * (Math.cos(t * 0.7 + 1.3) * 0.6 + Math.sin(t * 0.29) * 0.4);
    return [x, y];
  }
  const dur = d.durSec ?? 4;
  const p = Math.min(1, t / dur);
  const eased = p < 1 ? 1 - Math.pow(1 - p, 2.2) : 1;
  const segs = d.points.length - 1;
  const f = eased * segs;
  const i = Math.min(segs - 1, Math.floor(f));
  const local = f - i;
  const [x0, y0] = d.points[i];
  const [x1, y1] = d.points[i + 1];
  return [x0 + (x1 - x0) * local, y0 + (y1 - y0) * local];
};

/** Motion-graphic unit dots over the aerial — free-drift or path-following,
 * with pulse ring, optional label and dashed trail. Coordinates in % of frame. */
export const UnitDots: React.FC<{dots: UnitDot[]}> = ({dots}) => {
  const frame = useCurrentFrame();
  const {fps, width, height} = useVideoConfig();
  const tSec = frame / fps;
  return (
    <div style={{position: "absolute", inset: 0, pointerEvents: "none"}}>
      <svg width={width} height={height} style={{position: "absolute", inset: 0}}>
        {dots.map((d, k) => {
          const started = tSec >= (d.startSec ?? 0);
          if (!started) return null;
          const col = d.color ?? C.cyan;
          const [xp, yp] = pos(d, tSec);
          const x = (xp / 100) * width;
          const y = (yp / 100) * height;
          const pulse = 1 + 0.35 * Math.sin((tSec - (d.startSec ?? 0)) * 4.4);
          const appear = interpolate(tSec, [(d.startSec ?? 0), (d.startSec ?? 0) + 0.4], [0, 1], {
            extrapolateLeft: "clamp", extrapolateRight: "clamp"});
          return (
            <g key={k} opacity={appear}>
              {d.mode === "path" && d.trail !== false ? (
                <polyline
                  points={d.points.map(([px, py], i2) => {
                    // trail only up to current progress: recompute dot position per segment
                    return `${(px / 100) * width},${(py / 100) * height}`;
                  }).join(" ")}
                  fill="none" stroke={col} strokeOpacity={0.35}
                  strokeWidth={3} strokeDasharray="10 9"
                  style={{clipPath: `circle(${Math.hypot(x, y) + Math.max(width, height)}px at ${x}px ${y}px)`}}
                />
              ) : null}
              <circle cx={x} cy={y} r={16 * pulse} fill={col} opacity={0.18} />
              <circle cx={x} cy={y} r={9} fill={col} stroke="#0b0b0c" strokeWidth={2.5} />
              {d.label ? (
                <text x={x + 18} y={y - 14} fill="#f4f4f2" fontFamily={F.label} fontWeight={800}
                      fontSize={22} style={{textTransform: "uppercase", letterSpacing: "0.06em",
                      paintOrder: "stroke", stroke: "rgba(0,0,0,0.85)", strokeWidth: 5}}>
                  {d.label}
                </text>
              ) : null}
            </g>
          );
        })}
      </svg>
    </div>
  );
};
