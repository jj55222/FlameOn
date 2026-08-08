import React from "react";
import {AbsoluteFill, interpolate, useCurrentFrame, useVideoConfig} from "remotion";
import {C, F} from "../motifs/theme";
import {BLUE, MagLens} from "./MagLens";

/** TRUE CRIME DR — logo lockup. Heavy white caps; the DR monogram lives inside
 * the detective's magnifying glass (red/blue rim arcs = the light DNA).
 * Firm and investigative, not horror. */
export const LogoLockup: React.FC<{animate?: boolean; scale?: number; transparent?: boolean}> = ({animate = true, scale = 1, transparent = false}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const t = frame / fps;
  const spin = animate ? t * 140 : 22;
  const inP = animate ? interpolate(t, [0, 0.9], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"}) : 1;
  return (
    <AbsoluteFill style={{background: transparent ? undefined : "#060607", alignItems: "center", justifyContent: "center"}}>
      <svg width={0} height={0}><defs>
        <filter id="logo-grain"><feTurbulence type="fractalNoise" baseFrequency="0.7" numOctaves="2"/><feColorMatrix type="matrix" values="0 0 0 0 1 0 0 0 0 1 0 0 0 0 1 0 0 0 0.045 0"/></filter>
      </defs></svg>
      <div style={{display: "flex", alignItems: "center", gap: 44 * scale, opacity: inP, transform: `translateY(${(1 - inP) * 30}px)`}}>
        <div>
          <div style={{fontFamily: F.headline, color: C.white, fontSize: 168 * scale, letterSpacing: "0.02em", lineHeight: 0.98}}>TRUE</div>
          <div style={{fontFamily: F.headline, color: C.white, fontSize: 168 * scale, letterSpacing: "0.02em", lineHeight: 0.98}}>CRIME</div>
        </div>
        <MagLens size={330 * scale} spin={spin} glow={1} monogram="DR" tilt={-8} />
      </div>
      <div style={{display: "flex", alignItems: "center", gap: 22 * scale, marginTop: 30 * scale, opacity: inP}}>
        <div style={{width: 150 * scale, height: 3, background: C.red}} />
        <div style={{fontFamily: F.label, fontWeight: 800, letterSpacing: "0.42em", color: C.white, fontSize: 25 * scale}}>
          EVERY&nbsp;CASE&nbsp;ON&nbsp;RECORD
        </div>
        <div style={{width: 150 * scale, height: 3, background: BLUE}} />
      </div>
      <AbsoluteFill style={{filter: "url(#logo-grain)", pointerEvents: "none"}} />
    </AbsoluteFill>
  );
};
