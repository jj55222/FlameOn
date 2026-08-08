import React from "react";
import {interpolate, useCurrentFrame, useVideoConfig} from "remotion";
import {T} from "./theme";

/** Character-by-character type-on. The core primitive of the visual language:
 * headlines, labels, quotes and handwriting all enter through this. */
export const TypeOn: React.FC<{
  text: string;
  startSec?: number;
  charSec?: number;
  style?: React.CSSProperties;
  caret?: boolean;
  /** phrases to render in emphasis color; matched case-insensitively */
  redPhrases?: string[];
  redColor?: string;
}> = ({text, startSec = 0, charSec = T.typeCharSec, style, caret = false, redPhrases = [], redColor = "#d81f26"}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const t = frame / fps - startSec;
  const n = t <= 0 ? 0 : Math.min(text.length, Math.floor(t / charSec));
  const shown = text.slice(0, n);

  // mark emphasis ranges on the full text, then clip to what's typed
  const ranges: Array<[number, number]> = [];
  for (const p of redPhrases) {
    const i = text.toLowerCase().indexOf(p.toLowerCase());
    if (i >= 0) ranges.push([i, i + p.length]);
  }
  const parts: Array<{s: string; red: boolean}> = [];
  let cursor = 0;
  for (const [a, b] of ranges.sort((x, y) => x[0] - y[0])) {
    if (a > cursor) parts.push({s: text.slice(cursor, a), red: false});
    parts.push({s: text.slice(a, b), red: true});
    cursor = b;
  }
  parts.push({s: text.slice(cursor), red: false});

  let consumed = 0;
  const blink = caret && n < text.length && Math.floor(frame / (fps / 4)) % 2 === 0;
  return (
    <span style={style}>
      {parts.map((p, i) => {
        const take = Math.max(0, Math.min(p.s.length, n - consumed));
        consumed += p.s.length;
        return (
          <span key={i} style={p.red ? {color: redColor, fontWeight: 700} : undefined}>
            {p.s.slice(0, take)}
          </span>
        );
      })}
      {blink ? <span style={{opacity: 0.9}}>▌</span> : null}
    </span>
  );
};

/** Fade+rise build for cards/blocks, obeying the ~1s build contract. */
export const Build: React.FC<{
  startSec?: number;
  durSec?: number;
  from?: "up" | "left" | "none";
  children: React.ReactNode;
  style?: React.CSSProperties;
}> = ({startSec = 0, durSec = T.buildSec, from = "up", children, style}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const p = interpolate(frame / fps, [startSec, startSec + durSec], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const ease = 1 - Math.pow(1 - p, 3);
  const shift = 24 * (1 - ease);
  const tf = from === "up" ? `translateY(${shift}px)` : from === "left" ? `translateX(${-shift}px)` : "none";
  return <div style={{...style, opacity: ease, transform: tf}}>{children}</div>;
};

/** Draw-on dashed line (SVG), for evidence connectors and map paths. */
export const DrawPath: React.FC<{
  d: string;
  startSec?: number;
  durSec?: number;
  stroke?: string;
  strokeWidth?: number;
  dashed?: boolean;
  width: number;
  height: number;
}> = ({d, startSec = 0, durSec = 1.2, stroke = "#d81f26", strokeWidth = 4, dashed = true, width, height}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const p = interpolate(frame / fps, [startSec, startSec + durSec], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const L = 4000;
  return (
    <svg width={width} height={height} style={{position: "absolute", inset: 0, overflow: "visible"}}>
      <path
        d={d}
        fill="none"
        stroke={stroke}
        strokeWidth={strokeWidth}
        strokeDasharray={dashed ? `14 12` : `${L}`}
        strokeDashoffset={dashed ? undefined : L * (1 - p)}
        opacity={dashed ? undefined : 1}
        pathLength={dashed ? undefined : L}
        style={dashed ? {clipPath: `inset(0 ${100 - p * 100}% 0 0)`} : undefined}
      />
    </svg>
  );
};
