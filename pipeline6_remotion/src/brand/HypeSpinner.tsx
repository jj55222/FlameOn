import React from "react";
import {AbsoluteFill, interpolate, useCurrentFrame, useVideoConfig} from "remotion";
import {C} from "../motifs/theme";
import {BLUE, MagLens} from "./MagLens";
import {LogoLockup} from "./Logo";

/** Hype opener (~4.5s): the beacon ball rolls in from the left, spins up as red/
 * blue arcs accelerate (police-light strobe builds), white flash — slam to logo. */
export const HypeSpinner: React.FC<{transparent?: boolean}> = ({transparent = false}) => {
  const frame = useCurrentFrame();
  const {fps, width, height} = useVideoConfig();
  const t = frame / fps;

  const rollIn = interpolate(t, [0, 1.1], [-0.25, 0.5], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const speed = interpolate(t, [0, 1.1, 2.8], [90, 260, 1400], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const spin = speed * t;
  const grow = interpolate(t, [1.1, 2.8], [1, 2.1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const strobeOn = t > 1.6 && t < 2.9;
  const strobe = strobeOn ? (Math.floor(t * 12) % 2 === 0 ? C.red : BLUE) : "transparent";
  const flash = interpolate(t, [2.8, 2.95, 3.15], [0, 1, 0], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const logoIn = t >= 2.95;
  const S = 300;

  return (
    <AbsoluteFill style={{background: transparent ? undefined : "#060607", overflow: "hidden"}}>
      {!logoIn ? (
        <>
          {/* alternating wall-wash — the police-light room feel */}
          <AbsoluteFill style={{background: `radial-gradient(ellipse at ${rollIn * 100}% 60%, ${strobe}22 0%, transparent 55%)`}} />
          <div style={{position: "absolute", left: `${rollIn * 100}%`, top: "50%", transform: `translate(-50%,-50%) scale(${grow})`}}>
            <MagLens size={S} spin={spin} glow={Math.min(1.6, 0.4 + t * 0.5)} monogram="DR" tilt={spin * 0.55} />
          </div>
          {/* ground reflection */}
          <div style={{position: "absolute", left: `${rollIn * 100}%`, top: "50%", transform: `translate(-50%, ${S * grow * 0.42}px) scaleY(-0.35) scale(${grow})`, opacity: 0.22, filter: "blur(6px)"}}>
            <MagLens size={S} spin={spin} glow={0} monogram="DR" tilt={spin * 0.55} />
          </div>
        </>
      ) : (
        <LogoLockup animate={false} scale={0.9} transparent={transparent} />
      )}
      <AbsoluteFill style={{background: "#fff", opacity: flash, pointerEvents: "none"}} />
    </AbsoluteFill>
  );
};
