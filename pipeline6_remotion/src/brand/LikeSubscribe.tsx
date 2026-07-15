import React from "react";
import {AbsoluteFill, interpolate, useCurrentFrame, useVideoConfig} from "remotion";
import {TypeOn} from "../motifs/TypeOn";
import {C, F} from "../motifs/theme";
import {Beacon, BLUE} from "./Beacon";

/** Like & Subscribe sting (~6s): beacon rolls across, kicks a thumbs-up pop,
 * SUBSCRIBE pill sweeps red->blue, bell swings; red/blue underglow alternates. */
export const LikeSubscribe: React.FC = () => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const t = frame / fps;
  const roll = interpolate(t, [0, 1.4], [-15, 32], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const thumbPop = interpolate(t, [1.5, 1.75, 1.9], [0, 1.25, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const pill = interpolate(t, [2.4, 3.4], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const bell = t > 3.6 ? Math.sin((t - 3.6) * 14) * Math.max(0, 1 - (t - 3.6) / 1.2) * 22 : 0;
  const glow = Math.floor(t * 5) % 2 === 0 ? C.red : BLUE;
  return (
    <AbsoluteFill style={{background: "#060607", alignItems: "center", justifyContent: "center"}}>
      <AbsoluteFill style={{background: `radial-gradient(ellipse at 50% 92%, ${glow}26 0%, transparent 50%)`}} />
      <div style={{position: "absolute", left: `${roll}%`, top: "34%", transform: "translate(-50%,-50%)"}}>
        <Beacon size={170} spin={t * 300} glow={1} ball="black" />
      </div>
      {/* thumbs up */}
      <div style={{position: "absolute", left: "50%", top: "33%", transform: `translate(-50%,-50%) scale(${thumbPop})`, opacity: thumbPop > 0 ? 1 : 0}}>
        <svg width={150} height={150} viewBox="0 0 24 24">
          <path d="M2 21h4V9H2v12zM22 10c0-1.1-.9-2-2-2h-6.3l1-4.6.03-.32c0-.41-.17-.79-.44-1.06L13.2 1 6.6 7.6C6.2 7.9 6 8.4 6 9v10c0 1.1.9 2 2 2h9c.83 0 1.54-.5 1.84-1.22l3.02-7.05c.09-.23.14-.47.14-.73v-2z"
                fill="#f4f4f2" stroke={C.red} strokeWidth={0.8} />
        </svg>
        <div style={{textAlign: "center", fontFamily: F.headline, color: C.white, fontSize: 34, letterSpacing: "0.08em"}}>
          <TypeOn text="LIKE" startSec={1.8} charSec={0.05} />
        </div>
      </div>
      {/* subscribe pill */}
      <div style={{position: "absolute", left: "50%", top: "62%", transform: "translate(-50%,-50%)", width: 640, height: 110, borderRadius: 55, border: `3px solid ${C.cardEdge}`, overflow: "hidden", background: "#101013"}}>
        <div style={{position: "absolute", inset: 0, width: `${pill * 100}%`, background: `linear-gradient(90deg, ${C.red} 0%, ${C.redDeep} 45%, ${BLUE} 100%)`}} />
        <div style={{position: "absolute", inset: 0, display: "flex", alignItems: "center", justifyContent: "center", gap: 22}}>
          <span style={{fontFamily: F.headline, fontSize: 54, color: C.white, letterSpacing: "0.1em"}}>
            <TypeOn text="SUBSCRIBE" startSec={2.6} charSec={0.045} />
          </span>
          <svg width={54} height={54} viewBox="0 0 24 24" style={{transform: `rotate(${bell}deg)`, transformOrigin: "50% 12%"}}>
            <path d="M12 22c1.1 0 2-.9 2-2h-4c0 1.1.9 2 2 2zm6-6v-5c0-3.07-1.63-5.64-4.5-6.32V4c0-.83-.67-1.5-1.5-1.5S10.5 3.17 10.5 4v.68C7.64 5.36 6 7.92 6 11v5l-2 2v1h16v-1l-2-2z" fill="#f4f4f2" />
          </svg>
        </div>
      </div>
      <div style={{position: "absolute", bottom: 90, fontFamily: F.label, fontWeight: 700, letterSpacing: "0.24em", color: C.dim, fontSize: 20}}>
        <TypeOn text="NEW CASES EVERY WEEK" startSec={4.2} charSec={0.03} />
      </div>
    </AbsoluteFill>
  );
};
