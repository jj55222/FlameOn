import React from "react";
import {C, F} from "../motifs/theme";

export const BLUE = "#2456f0";

/** True Crime DR brand device: a detective's magnifying glass. The lens rim
 * carries red/blue orbit arcs (police-light DNA); "DR" sits inside the lens.
 * spin animates the arcs; tilt rocks the handle for roll-in moves. */
export const MagLens: React.FC<{
  size: number;                // lens outer diameter (handle extends beyond)
  spin: number;
  glow?: number;
  monogram?: string;           // "DR" (empty string for plain glass)
  tilt?: number;               // degrees, whole-glass rotation
}> = ({size, spin, glow = 1, monogram = "DR", tilt = 0}) => {
  const r = size / 2;
  const pad = size * 0.3;      // room for the handle
  const cx = r + pad / 2;
  const cy = r + pad / 2;
  const arc = (start: number, sweep: number, color: string, width: number, radius: number, g: number) => {
    const a0 = ((start - 90) * Math.PI) / 180;
    const a1 = ((start + sweep - 90) * Math.PI) / 180;
    return (
      <path
        d={`M ${cx + radius * Math.cos(a0)} ${cy + radius * Math.sin(a0)} A ${radius} ${radius} 0 ${sweep > 180 ? 1 : 0} 1 ${cx + radius * Math.cos(a1)} ${cy + radius * Math.sin(a1)}`}
        fill="none" stroke={color} strokeWidth={width} strokeLinecap="round"
        style={g > 0 ? {filter: `drop-shadow(0 0 ${9 * g}px ${color})`} : undefined}
      />
    );
  };
  const rimR = r * 0.88;
  return (
    <svg width={size + pad} height={size + pad} style={{display: "block", transform: `rotate(${tilt}deg)`}}>
      <defs>
        <radialGradient id="lensglass" cx="36%" cy="30%">
          <stop offset="0%" stopColor="#ffffff" stopOpacity={0.16} />
          <stop offset="55%" stopColor="#9db4ff" stopOpacity={0.05} />
          <stop offset="100%" stopColor="#0a0a0c" stopOpacity={0.4} />
        </radialGradient>
      </defs>
      {/* handle at 4:30 */}
      <g transform={`rotate(45 ${cx} ${cy})`}>
        <rect x={cx - size * 0.045} y={cy + rimR * 0.92} width={size * 0.09} height={size * 0.46} rx={size * 0.045} fill="#e8e8ea" />
        <rect x={cx - size * 0.045} y={cy + rimR * 0.92 + size * 0.3} width={size * 0.09} height={size * 0.16} rx={size * 0.045} fill={C.red} />
      </g>
      {/* lens */}
      <circle cx={cx} cy={cy} r={rimR} fill="url(#lensglass)" stroke="#f4f4f2" strokeWidth={size * 0.028} />
      {/* glint */}
      <ellipse cx={cx - rimR * 0.38} cy={cy - rimR * 0.42} rx={rimR * 0.3} ry={rimR * 0.12} fill="#ffffff" opacity={0.28} transform={`rotate(-28 ${cx - rimR * 0.38} ${cy - rimR * 0.42})`} />
      {/* police-light arcs on the rim */}
      <g transform={`rotate(${spin} ${cx} ${cy})`}>{arc(0, 115, C.red, size * 0.05, rimR, glow)}</g>
      <g transform={`rotate(${-spin * 1.15 + 180} ${cx} ${cy})`}>{arc(0, 115, BLUE, size * 0.05, rimR, glow)}</g>
      <g transform={`rotate(${spin * 1.55 + 95} ${cx} ${cy})`}>{arc(0, 22, "#f4f4f2", size * 0.026, rimR, 0)}</g>
      {monogram ? (
        <text x={cx} y={cy + size * 0.115} textAnchor="middle" fill="#f4f4f2"
              fontFamily={F.headline} fontWeight={900} fontSize={size * 0.34}
              style={{letterSpacing: "0.02em"}}>
          {monogram}
        </text>
      ) : null}
    </svg>
  );
};
