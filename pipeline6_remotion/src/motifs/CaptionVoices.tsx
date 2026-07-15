import React from "react";
import {useCurrentFrame, useVideoConfig} from "remotion";
import {TypeOn} from "./TypeOn";
import {C, F} from "./theme";

/** Motif 9d — the three caption "voices" so the audience always knows who speaks.
 * 911 caller/dispatch: blue with electric waveform · narrator: white dash-prefixed ·
 * plus the ACTUAL CASE FOOTAGE corner badge. All type on (~1s). */

export const Caption911: React.FC<{text: string; startSec?: number; bottom?: number}> = ({text, startSec = 0, bottom = 120}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const t = Math.max(0, frame / fps - startSec);
  const amp = t > 0 ? 1 : 0;
  const wave = Array.from({length: 60}, (_, i) => {
    const x = i * 14;
    const y = 26 + Math.sin(i * 0.9 + frame * 0.5) * 14 * Math.sin(i * 0.23 + frame * 0.21) * amp;
    return `${i === 0 ? "M" : "L"}${x},${y}`;
  }).join(" ");
  return (
    <div style={{position: "absolute", left: 0, right: 0, bottom, textAlign: "center"}}>
      <svg width={840} height={52} style={{opacity: 0.85, filter: `drop-shadow(0 0 12px ${C.blue911})`}}>
        <path d={wave} stroke={C.blue911} strokeWidth={3} fill="none" />
      </svg>
      <div style={{fontFamily: F.label, fontWeight: 800, fontSize: 44, color: C.blue911, textShadow: "0 2px 14px rgba(0,0,0,0.9)"}}>
        <TypeOn text={text} startSec={startSec} charSec={0.02} />
      </div>
    </div>
  );
};

export const CaptionNarrator: React.FC<{text: string; startSec?: number; bottom?: number}> = ({text, startSec = 0, bottom = 110}) => (
  <div style={{position: "absolute", left: 0, right: 0, bottom, textAlign: "center"}}>
    <div style={{display: "inline-block", background: "rgba(0,0,0,0.72)", padding: "10px 22px", fontFamily: F.label, fontWeight: 600, fontSize: 40, color: C.white}}>
      <TypeOn text={`— ${text}`} startSec={startSec} charSec={0.018} />
    </div>
  </div>
);

/** Red bracketed non-speech caption: [ gunshots ], [ screaming ], [ throwing up ]. */
export const CaptionSFX: React.FC<{text: string; startSec?: number; bottom?: number}> = ({text, startSec = 0, bottom = 110}) => (
  <div style={{position: "absolute", left: 0, right: 0, bottom, textAlign: "center"}}>
    <div style={{display: "inline-block", fontFamily: F.label, fontWeight: 800, fontSize: 42, color: C.red, textShadow: "0 2px 14px rgba(0,0,0,0.95)", letterSpacing: "0.04em"}}>
      <TypeOn text={`[ ${text} ]`} startSec={startSec} charSec={0.03} />
    </div>
  </div>
);

export const FootageBadge: React.FC<{label?: string}> = ({label = "ACTUAL CASE FOOTAGE"}) => (
  <div style={{position: "absolute", top: 42, left: 42, background: "rgba(0,0,0,0.78)", color: C.white, fontFamily: F.label, fontWeight: 800, letterSpacing: "0.08em", fontSize: 24, padding: "8px 14px", borderLeft: `4px solid ${C.red}`}}>
    {label}
  </div>
);
