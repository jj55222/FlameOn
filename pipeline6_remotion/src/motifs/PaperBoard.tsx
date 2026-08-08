import React from "react";
import {AbsoluteFill, Img, staticFile} from "remotion";
import {Build, TypeOn} from "./TypeOn";
import {C, F} from "./theme";

/** Motif 7 — case-file paper board: taped polaroid drops with a stick-wobble,
 * handwriting annotations write themselves (EWU person-intro style). */
export const PaperBoard: React.FC<{
  photo?: string;                 // bundle photo (booking/plain clothes)
  name: string;                   // handwritten above the photo
  role: string;                   // handwritten beside ("Ex-boyfriend")
  caption?: string;               // narrator caption bar
  startSec?: number;
}> = ({photo, name, role, caption, startSec = 0.2}) => (
  <AbsoluteFill style={{background: "#a8a5a0"}}>
    {/* procedural paper texture */}
    <svg width={0} height={0}><defs>
      <filter id="paper"><feTurbulence type="fractalNoise" baseFrequency="0.045" numOctaves="4" stitchTiles="stitch"/><feColorMatrix type="matrix" values="0 0 0 0 0.72 0 0 0 0 0.71 0 0 0 0 0.69 0 0 0 0.9 0"/><feComposite operator="over" in2="SourceGraphic"/></filter>
      <filter id="paper-grain"><feTurbulence type="fractalNoise" baseFrequency="0.8" numOctaves="2"/><feColorMatrix type="matrix" values="0 0 0 0 0.4 0 0 0 0 0.39 0 0 0 0 0.37 0 0 0 0.12 0"/></filter>
    </defs></svg>
    <AbsoluteFill style={{filter: "url(#paper)"}} />
    <AbsoluteFill style={{filter: "url(#paper-grain)"}} />
    <AbsoluteFill style={{background: "radial-gradient(ellipse at center, rgba(0,0,0,0) 55%, rgba(0,0,0,0.38) 100%)"}} />

    <div style={{position: "absolute", left: "50%", top: "12%", transform: "translateX(-50%)", textAlign: "center"}}>
      <div style={{fontFamily: F.hand, fontSize: 58, color: C.ink}}>
        <TypeOn text={name} startSec={startSec + 0.9} charSec={0.05} />
      </div>
    </div>

    <Build startSec={startSec} durSec={0.55} from="up" style={{position: "absolute", left: "50%", top: "22%", transform: "translateX(-50%)"}}>
      <div style={{background: "#f6f4ef", padding: 12, border: "2px solid #17151280", transform: "rotate(-2deg)", boxShadow: "0 22px 60px rgba(0,0,0,0.45)"}}>
        {/* tape */}
        <div style={{position: "absolute", top: -14, left: "50%", transform: "translateX(-50%) rotate(3deg)", width: 120, height: 30, background: "rgba(230,225,205,0.8)", boxShadow: "0 2px 8px rgba(0,0,0,0.25)"}} />
        {photo ? (
          <Img src={staticFile(photo)} style={{width: 340, height: 420, objectFit: "cover", filter: "saturate(0.9)"}} />
        ) : (
          <div style={{width: 340, height: 420, background: "#dedbd4", display: "flex", alignItems: "center", justifyContent: "center", color: "#8f1216", fontFamily: F.headline, fontSize: 110}}>?</div>
        )}
      </div>
    </Build>

    <div style={{position: "absolute", left: "66%", top: "48%", fontFamily: F.hand, fontSize: 46, color: C.ink, transform: "rotate(-4deg)"}}>
      <TypeOn text={role} startSec={startSec + 1.7} charSec={0.06} />
    </div>

    {caption ? (
      <div style={{position: "absolute", left: 0, right: 0, bottom: 90, textAlign: "center"}}>
        <div style={{display: "inline-block", background: "rgba(0,0,0,0.78)", padding: "10px 22px", fontFamily: F.label, fontWeight: 600, fontSize: 36, color: "#fff"}}>
          <TypeOn text={caption} startSec={startSec + 2.3} charSec={0.016} />
        </div>
      </div>
    ) : null}
  </AbsoluteFill>
);
