import React from "react";
import {AbsoluteFill, Img, interpolate, staticFile, useCurrentFrame, useVideoConfig} from "remotion";
import {Build, TypeOn} from "./TypeOn";
import {C, F, kicker, sourceLine} from "./theme";

/** Motif 2 — narration-driven evidence board ("THE CHAIN").
 *
 * DESIGN CONTRACT (operator 2026-07-14): the board visualizes what the narration
 * is saying AS it is said. Each card/link carries appearSec — the props adapter
 * maps narration beats to these times ("the gun was hidden in the closet" →
 * suspect card connects to closet-gun card at that word). Card images come from
 * the CASE BUNDLE (booking/plain-clothes photo, packet-strip frames, doc crops);
 * a missing artifact renders as a red ? placeholder, never a stand-in photo. */

export type BoardCard = {
  id: string;
  label: string;             // "CURTIS HARRIS"
  sublabel?: string;         // "suspect · plain clothes DMV photo"
  image?: string;            // staticFile path from the case bundle; absent -> red ?
  at: [number, number];      // percent [x, y] of frame (card center)
  appearSec: number;
  w?: number;                // px width, default 240
};

export type BoardLink = {
  from: string;
  to: string;
  appearSec: number;
  label?: string;            // optional link caption ("found in closet")
};

export type EvidenceBoardProps = {
  kickerText?: string;       // "THE EVIDENCE"
  title: string;             // "THE CHAIN"
  cards: BoardCard[];
  links: BoardLink[];
  verdict?: {text: string; appearSec: number};   // "IA SUSTAINED — DISHONESTY"
  source: string;
};

const cardPos = (c: BoardCard, w: number, h: number) => ({
  x: (c.at[0] / 100) * w,
  y: (c.at[1] / 100) * h,
});

export const EvidenceBoard: React.FC<EvidenceBoardProps> = (p) => {
  const frame = useCurrentFrame();
  const {fps, width, height} = useVideoConfig();
  const t = frame / fps;
  const byId = Object.fromEntries(p.cards.map((c) => [c.id, c]));

  return (
    <AbsoluteFill style={{backgroundColor: C.bg, fontFamily: F.label}}>
      {/* subtle board texture */}
      <svg width={0} height={0}><defs>
        <filter id="board-grain"><feTurbulence type="fractalNoise" baseFrequency="0.9" numOctaves="2" stitchTiles="stitch"/><feColorMatrix type="matrix" values="0 0 0 0 0.06 0 0 0 0 0.06 0 0 0 0 0.065 0 0 0 0.5 0"/></filter>
      </defs></svg>
      <AbsoluteFill style={{filter: "url(#board-grain)", opacity: 0.5}} />

      <div style={{position: "absolute", top: 56, left: 84}}>
        <div style={kicker}><TypeOn text={p.kickerText ?? "THE EVIDENCE"} startSec={0.1} charSec={0.02} /></div>
        <div style={{fontFamily: F.headline, color: C.white, fontSize: 74, textTransform: "uppercase"}}>
          <TypeOn text={p.title} startSec={0.5} charSec={0.05} caret />
        </div>
      </div>

      {/* links draw beneath cards */}
      <svg width={width} height={height} style={{position: "absolute", inset: 0}}>
        {p.links.map((l, i) => {
          const a = byId[l.from]; const b = byId[l.to];
          if (!a || !b) return null;
          const pr = interpolate(t, [l.appearSec, l.appearSec + 0.9], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
          if (pr <= 0) return null;
          const A = cardPos(a, width, height); const B = cardPos(b, width, height);
          const mx = A.x + (B.x - A.x) * pr; const my = A.y + (B.y - A.y) * pr;
          return (
            <g key={i}>
              <line x1={A.x} y1={A.y} x2={mx} y2={my} stroke={C.red} strokeWidth={3.5} strokeDasharray="12 10" opacity={0.9} />
              <circle cx={A.x} cy={A.y} r={7} fill={C.red} />
              {pr >= 1 ? <circle cx={B.x} cy={B.y} r={7} fill={C.red} /> : null}
              {l.label && pr >= 1 ? (
                <text x={(A.x + B.x) / 2} y={(A.y + B.y) / 2 - 12} fill={C.dim} fontFamily={F.mono} fontSize={20} textAnchor="middle"
                      style={{paintOrder: "stroke", stroke: "rgba(10,10,11,0.9)", strokeWidth: 6}}>
                  {l.label}
                </text>
              ) : null}
            </g>
          );
        })}
      </svg>

      {p.cards.map((c) => {
        const w = c.w ?? 240;
        const pos = cardPos(c, width, height);
        const wob = ((c.id.charCodeAt(0) + c.id.length) % 7) - 3; // stable pseudo-random tilt
        return (
          <Build key={c.id} startSec={c.appearSec} durSec={0.6} style={{position: "absolute", left: pos.x - w / 2, top: pos.y - (w * 0.75) / 2}}>
            <div style={{width: w, background: "#101012", border: `1px solid ${C.cardEdge}`, padding: 8, transform: `rotate(${wob}deg)`, boxShadow: "0 16px 44px rgba(0,0,0,0.65)"}}>
              {c.image ? (
                <Img src={staticFile(c.image)} style={{width: "100%", height: w * 0.62, objectFit: "cover", filter: "saturate(0.85) contrast(1.05)"}} />
              ) : (
                <div style={{width: "100%", height: w * 0.62, background: "#1b1b1e", display: "flex", alignItems: "center", justifyContent: "center", color: C.red, fontFamily: F.headline, fontSize: 64}}>?</div>
              )}
              <div style={{color: C.white, fontWeight: 800, fontSize: 21, marginTop: 8, textTransform: "uppercase", letterSpacing: "0.03em"}}>
                <TypeOn text={c.label} startSec={c.appearSec + 0.35} charSec={0.02} />
              </div>
              {c.sublabel ? <div style={{color: C.dim, fontFamily: F.mono, fontSize: 16, marginTop: 4}}>{c.sublabel}</div> : null}
            </div>
          </Build>
        );
      })}

      {p.verdict ? (
        <Build startSec={p.verdict.appearSec} from="up" style={{position: "absolute", left: 0, right: 0, bottom: 130, textAlign: "center"}}>
          <div style={{display: "inline-block", background: C.redDeep, border: `1px solid ${C.red}`, color: C.white, fontFamily: F.headline, fontSize: 44, letterSpacing: "0.04em", padding: "14px 34px", textTransform: "uppercase"}}>
            {p.verdict.text}
          </div>
        </Build>
      ) : null}

      <div style={{position: "absolute", left: 84, bottom: 48, ...sourceLine}}>
        <span style={{color: C.red}}>▪ </span>SOURCE — {p.source}
      </div>
    </AbsoluteFill>
  );
};
