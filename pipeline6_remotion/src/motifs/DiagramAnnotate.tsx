import React from "react";
import {AbsoluteFill, Img, interpolate, staticFile, useCurrentFrame, useVideoConfig} from "remotion";
import {TypeOn} from "./TypeOn";
import {C, F, sourceLine} from "./theme";

export type DiagramMarker = {
  at: [number, number];        // percent on the page image
  appearSec: number;
  label?: string;              // "bullet hole · kitchen wall"
  color?: string;
};

/** Motif 12 — annotate a REAL case-file diagram page (floor plan, body chart):
 * page floats with slight parallax tilt; markers pulse on at narration anchors;
 * soft glow region optional. Page images come from the case PDF — never drawn. */
export const DiagramAnnotate: React.FC<{
  page: string;                 // staticFile page image (rasterized from case PDF)
  markers: DiagramMarker[];
  legend?: string;              // "● = bullet holes (CSI diagram, p.1)"
  source: string;
  glowAt?: [number, number];    // percent center of a soft glow region
}> = ({page, markers, legend, source, glowAt}) => {
  const frame = useCurrentFrame();
  const {fps, width, height} = useVideoConfig();
  const t = frame / fps;
  const drift = Math.sin(t * 0.4) * 0.6;
  return (
    <AbsoluteFill style={{background: "radial-gradient(ellipse at center, #232326 0%, #0a0a0b 75%)", alignItems: "center", justifyContent: "center"}}>
      <div style={{position: "relative", transform: `perspective(1400px) rotateX(${4 + drift}deg) rotateZ(${-1.2 + drift * 0.4}deg)`, boxShadow: "0 50px 140px rgba(0,0,0,0.85)"}}>
        <Img src={staticFile(page)} style={{width: Math.min(width * 0.72, 1380), display: "block", filter: "brightness(0.96) contrast(1.04)"}} />
        {glowAt ? (
          <div style={{position: "absolute", left: `${glowAt[0]}%`, top: `${glowAt[1]}%`, width: 340, height: 260, transform: "translate(-50%,-50%)", background: `radial-gradient(ellipse, ${C.cyan}33 0%, transparent 70%)`}} />
        ) : null}
        {markers.map((m, i) => {
          const a = interpolate(t, [m.appearSec, m.appearSec + 0.4], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
          if (a <= 0) return null;
          const pulse = 1 + 0.4 * Math.sin((t - m.appearSec) * 4.2);
          const col = m.color ?? C.cyan;
          return (
            <div key={i} style={{position: "absolute", left: `${m.at[0]}%`, top: `${m.at[1]}%`, transform: "translate(-50%,-50%)", opacity: a}}>
              <div style={{width: 26 * pulse, height: 26 * pulse, borderRadius: "50%", background: `${col}2e`, position: "absolute", left: "50%", top: "50%", transform: "translate(-50%,-50%)"}} />
              <div style={{width: 13, height: 13, borderRadius: "50%", background: col, boxShadow: `0 0 12px ${col}`}} />
              {m.label ? (
                <div style={{position: "absolute", left: 18, top: -8, whiteSpace: "nowrap", fontFamily: F.mono, fontSize: 17, color: C.white, background: "rgba(10,10,12,0.85)", padding: "3px 8px", border: `1px solid ${C.cardEdge}`}}>
                  <TypeOn text={m.label} startSec={m.appearSec + 0.3} charSec={0.02} />
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
      {legend ? (
        <div style={{position: "absolute", bottom: 104, fontFamily: F.mono, fontSize: 22, color: C.cyan}}>
          <TypeOn text={legend} startSec={0.8} charSec={0.02} />
        </div>
      ) : null}
      <div style={{position: "absolute", left: 72, bottom: 44, ...sourceLine}}>
        <span style={{color: C.red}}>▪ </span>SOURCE — {source}
      </div>
    </AbsoluteFill>
  );
};
