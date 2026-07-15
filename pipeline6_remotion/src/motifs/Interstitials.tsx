import React from "react";
import {AbsoluteFill, Img, interpolate, staticFile, useCurrentFrame, useVideoConfig} from "remotion";
import {TypeOn} from "./TypeOn";
import {C, F} from "./theme";

/** Motif 9a — branded progress interstitial (SolvedFiles "22%"). */
export const ProgressInterstitial: React.FC<{pct: number; brand?: string; accent?: string}> = ({pct, brand = "FLAMEON", accent = C.cyan}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const p = interpolate(frame / fps, [0.2, 1.6], [0, pct / 100], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  return (
    <AbsoluteFill style={{background: "#000", alignItems: "center", justifyContent: "center"}}>
      <div style={{fontFamily: F.label, fontWeight: 300, fontSize: 120, color: C.white, letterSpacing: "0.02em"}}>
        {Math.round(p * 100)}%
      </div>
      <div style={{width: "62%", height: 26, background: "#141416", borderRadius: 14, marginTop: 26, overflow: "hidden"}}>
        <div style={{width: `${p * 100}%`, height: "100%", background: accent, borderRadius: 14, boxShadow: `0 0 26px ${accent}`}} />
      </div>
      <div style={{fontFamily: F.label, fontWeight: 700, letterSpacing: "0.3em", color: C.white, fontSize: 26, marginTop: 30}}>
        {brand}
      </div>
    </AbsoluteFill>
  );
};

/** Motif 9b — distressed cinematic title card (grain + gradient sheen + hand side-words). */
export const TitleCard: React.FC<{wordmark: string; left?: string; right?: string; credit?: string}> = ({wordmark, left = "true", right = "Crime", credit}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const sheen = (frame / fps) * 18;
  return (
    <AbsoluteFill style={{background: "#050505", alignItems: "center", justifyContent: "center"}}>
      <svg width={0} height={0}><defs>
        <filter id="title-grain"><feTurbulence type="fractalNoise" baseFrequency="0.65" numOctaves="2"/><feColorMatrix type="matrix" values="0 0 0 0 1 0 0 0 0 1 0 0 0 0 1 0 0 0 0.06 0"/></filter>
      </defs></svg>
      <div style={{position: "absolute", left: "12%", fontFamily: F.hand, color: "#e8e6df", fontSize: 40}}>{left}</div>
      <div style={{position: "absolute", right: "12%", fontFamily: F.hand, color: "#e8e6df", fontSize: 40}}>{right}</div>
      <div style={{
        fontFamily: F.headline, fontSize: 190, letterSpacing: "0.02em", textTransform: "uppercase",
        backgroundImage: `linear-gradient(100deg, #efeee9 ${sheen - 30}%, ${C.gold} ${sheen}%, #efeee9 ${sheen + 30}%)`,
        WebkitBackgroundClip: "text", backgroundClip: "text", color: "transparent",
        filter: "drop-shadow(0 6px 30px rgba(0,0,0,0.8))",
      }}>
        <TypeOn text={wordmark} startSec={0.15} charSec={0.06} />
      </div>
      {credit ? (
        <div style={{position: "absolute", bottom: 64, fontFamily: F.label, color: "#9a9a96", fontSize: 22, letterSpacing: "0.12em", textTransform: "uppercase"}}>{credit}</div>
      ) : null}
      <AbsoluteFill style={{filter: "url(#title-grain)", pointerEvents: "none"}} />
    </AbsoluteFill>
  );
};

/** Motif 9c — COMING UP.. multicam teaser grid (frames from packet strips). */
export const ComingUpGrid: React.FC<{tiles: string[]; caption?: string}> = ({tiles, caption = "COMING UP.."}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  return (
    <AbsoluteFill style={{background: "#000"}}>
      <div style={{display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gridTemplateRows: "repeat(2, 1fr)", width: "100%", height: "100%", gap: 4}}>
        {tiles.slice(0, 6).map((f, i) => {
          const on = frame / fps >= i * 0.28;
          return (
            <div key={i} style={{overflow: "hidden", opacity: on ? 1 : 0, transition: "none"}}>
              <Img src={staticFile(f)} style={{width: "100%", height: "100%", objectFit: "cover", filter: "saturate(0.9)"}} />
            </div>
          );
        })}
      </div>
      <div style={{position: "absolute", left: 0, right: 0, bottom: 60, textAlign: "center", fontFamily: F.label, fontWeight: 900, fontSize: 56, color: "#fff", textShadow: "0 3px 16px rgba(0,0,0,0.95)", letterSpacing: "0.06em"}}>
        <TypeOn text={caption} startSec={1.4} charSec={0.05} />
      </div>
    </AbsoluteFill>
  );
};

/** Motif 8 — case-records artifact card with mandatory honesty strip when illustrative. */
export const RecordsArtifact: React.FC<{
  caseNo: string;
  rows: Array<[string, string]>;    // [["CAD Event","23-340576"], ...]
  illustrative?: boolean;
  startSec?: number;
}> = ({caseNo, rows, illustrative = true, startSec = 0.2}) => (
  <AbsoluteFill style={{background: C.bg, alignItems: "center", justifyContent: "center"}}>
    <div style={{width: 430, background: "#0f0f11", border: `1px solid ${C.cardEdge}`, borderRadius: 38, padding: "44px 34px", boxShadow: "0 30px 80px rgba(0,0,0,0.7)"}}>
      <div style={{textAlign: "center", color: C.red, fontFamily: F.label, fontWeight: 800, letterSpacing: "0.18em", fontSize: 20}}>
        <TypeOn text="CASE RECORDS" startSec={startSec} charSec={0.03} />
      </div>
      <div style={{textAlign: "center", color: C.dim, fontFamily: F.mono, fontSize: 17, marginTop: 6}}>Linked to {caseNo}</div>
      <div style={{marginTop: 26}}>
        {rows.map(([k, v], i) => (
          <div key={k} style={{display: "flex", justifyContent: "space-between", padding: "12px 2px", borderBottom: `1px solid ${C.cardEdge}`}}>
            <span style={{color: C.dim, fontFamily: F.mono, fontSize: 18}}>
              <TypeOn text={k} startSec={startSec + 0.7 + i * 0.5} charSec={0.02} />
            </span>
            <span style={{color: C.white, fontFamily: F.mono, fontSize: 18}}>
              <TypeOn text={v} startSec={startSec + 0.95 + i * 0.5} charSec={0.02} />
            </span>
          </div>
        ))}
      </div>
    </div>
    {illustrative ? (
      <div style={{position: "absolute", bottom: 84, background: C.redDeep, color: "#fff", fontFamily: F.label, fontWeight: 800, fontSize: 22, padding: "8px 18px", letterSpacing: "0.06em"}}>
        ILLUSTRATIVE TEMPLATE — no phone/social artifact in this case
      </div>
    ) : null}
  </AbsoluteFill>
);
