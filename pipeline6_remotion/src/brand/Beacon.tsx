import React from "react";
import {C} from "../motifs/theme";

export const BLUE = "#2456f0";

/** The brand device: a beacon ball — dark sphere wrapped by red and blue orbit
 * arcs with a white sweep gap. spin = rotation in degrees; glow scales lights. */
export const Beacon: React.FC<{
  size: number;
  spin: number;
  glow?: number;          // 0..1 light intensity
  ball?: "black" | "white";
}> = ({size, spin, glow = 1, ball = "black"}) => {
  const r = size / 2;
  const arc = (start: number, sweep: number, color: string, width: number, radius: number) => {
    const a0 = ((start - 90) * Math.PI) / 180;
    const a1 = ((start + sweep - 90) * Math.PI) / 180;
    const large = sweep > 180 ? 1 : 0;
    return (
      <path
        d={`M ${r + radius * Math.cos(a0)} ${r + radius * Math.sin(a0)} A ${radius} ${radius} 0 ${large} 1 ${r + radius * Math.cos(a1)} ${r + radius * Math.sin(a1)}`}
        fill="none" stroke={color} strokeWidth={width} strokeLinecap="round"
      />
    );
  };
  return (
    <svg width={size} height={size} style={{display: "block"}}>
      <defs>
        <radialGradient id="ballshade" cx="38%" cy="32%">
          <stop offset="0%" stopColor={ball === "black" ? "#3a3a40" : "#ffffff"} />
          <stop offset="70%" stopColor={ball === "black" ? "#101013" : "#dcdcdf"} />
          <stop offset="100%" stopColor={ball === "black" ? "#050506" : "#b8b8bd"} />
        </radialGradient>
      </defs>
      <circle cx={r} cy={r} r={r * 0.62} fill="url(#ballshade)" />
      {/* white equator sweep on the ball */}
      <g transform={`rotate(${spin * 0.6} ${r} ${r})`}>
        <ellipse cx={r} cy={r} rx={r * 0.62} ry={r * 0.2} fill="none" stroke="#f4f4f2" strokeOpacity={0.85} strokeWidth={size * 0.018} />
      </g>
      {/* orbiting light arcs */}
      <g transform={`rotate(${spin} ${r} ${r})`} style={{filter: glow > 0 ? `drop-shadow(0 0 ${10 * glow}px ${C.red})` : undefined}}>
        {arc(0, 120, C.red, size * 0.055, r * 0.86)}
      </g>
      <g transform={`rotate(${-spin * 1.18 + 180} ${r} ${r})`} style={{filter: glow > 0 ? `drop-shadow(0 0 ${10 * glow}px ${BLUE})` : undefined}}>
        {arc(0, 120, BLUE, size * 0.055, r * 0.86)}
      </g>
      {/* white counter-tick */}
      <g transform={`rotate(${spin * 1.6 + 90} ${r} ${r})`}>
        {arc(0, 26, "#f4f4f2", size * 0.03, r * 0.86)}
      </g>
    </svg>
  );
};
