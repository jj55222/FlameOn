import React from "react";
import {AbsoluteFill, interpolate, useCurrentFrame, useVideoConfig} from "remotion";
import {C, F} from "../motifs/theme";
import {Beacon, BLUE} from "./Beacon";

/** Channel logo lockup — FLAMEON with the beacon ball as the O.
 * Firm, civic, true-crime-serious: heavy condensed caps, thin rules, no horror. */
export const LogoLockup: React.FC<{animate?: boolean; scale?: number}> = ({animate = true, scale = 1}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const t = frame / fps;
  const spin = animate ? t * 160 : 28;
  const inP = animate ? interpolate(t, [0, 0.9], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"}) : 1;
  const S = 210 * scale;
  return (
    <AbsoluteFill style={{background: "#060607", alignItems: "center", justifyContent: "center"}}>
      <svg width={0} height={0}><defs>
        <filter id="logo-grain"><feTurbulence type="fractalNoise" baseFrequency="0.7" numOctaves="2"/><feColorMatrix type="matrix" values="0 0 0 0 1 0 0 0 0 1 0 0 0 0 1 0 0 0 0.045 0"/></filter>
      </defs></svg>
      <div style={{display: "flex", alignItems: "center", gap: 6 * scale, opacity: inP, transform: `translateY(${(1 - inP) * 30}px)`}}>
        <span style={{fontFamily: F.headline, color: C.white, fontSize: 200 * scale, letterSpacing: "0.015em", lineHeight: 1}}>FLAME</span>
        <div style={{position: "relative", top: 8 * scale}}>
          <Beacon size={S} spin={spin} glow={1} ball="black" />
        </div>
        <span style={{fontFamily: F.headline, color: C.white, fontSize: 200 * scale, letterSpacing: "0.015em", lineHeight: 1}}>N</span>
      </div>
      <div style={{display: "flex", alignItems: "center", gap: 22 * scale, marginTop: 26 * scale, opacity: inP}}>
        <div style={{width: 130 * scale, height: 3, background: C.red}} />
        <div style={{fontFamily: F.label, fontWeight: 800, letterSpacing: "0.42em", color: C.white, fontSize: 27 * scale}}>
          CASE&nbsp;FILES
        </div>
        <div style={{width: 130 * scale, height: 3, background: BLUE}} />
      </div>
      <AbsoluteFill style={{filter: "url(#logo-grain)", pointerEvents: "none"}} />
    </AbsoluteFill>
  );
};
