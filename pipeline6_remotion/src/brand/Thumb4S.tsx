import React from "react";
import {AbsoluteFill, Img, staticFile} from "remotion";
import {MagLens} from "./MagLens";

const RED = "#d81f26";
const BLUE = "#2456f0";
const ANTON = "'Anton','Arial Narrow','Impact',sans-serif";
const MONO = "'JetBrains Mono','SF Mono',Menlo,monospace";

/** YouTube thumbnail for the 4S Commons Drive OIS (1280x720).
 * Hero: Officer3 BWC 23:56:27 — arm thrust at the Jeep window during the volley.
 * Text: the 911 caller's own words (ties to the blue 911-card brand language). */
export const Thumb4S: React.FC = () => (
  <AbsoluteFill style={{background: "#060607", overflow: "hidden"}}>
    {/* hero frame — zoomed past the Axon burn-in, arm on the right third */}
    <Img src={staticFile("thumb4s/bg.png")} style={{
      position: "absolute", width: "114%", left: "-7%", top: "-12%",
    }} />
    {/* mask the Axon burn-in top-right */}
    <div style={{position: "absolute", right: 0, top: 0, width: 640, height: 130,
      background: "linear-gradient(180deg, rgba(4,5,8,1) 45%, rgba(4,5,8,0) 100%)"}} />
    {/* legibility gradients */}
    <div style={{position: "absolute", inset: 0,
      background: "linear-gradient(90deg, rgba(4,5,8,0.88) 0%, rgba(4,5,8,0.55) 34%, rgba(4,5,8,0) 62%)"}} />
    <div style={{position: "absolute", inset: 0,
      background: "linear-gradient(0deg, rgba(4,5,8,0.72) 0%, rgba(4,5,8,0) 38%)"}} />

    {/* ACTUAL CASE FOOTAGE badge */}
    <div style={{position: "absolute", top: 26, left: 28, display: "flex", alignItems: "center", gap: 12,
      background: "rgba(8,8,10,0.82)", border: "1px solid rgba(255,255,255,0.16)", padding: "10px 18px"}}>
      <div style={{width: 13, height: 13, borderRadius: 13, background: RED}} />
      <span style={{fontFamily: MONO, fontSize: 21, letterSpacing: 4, color: "#f0f0ee"}}>ACTUAL CASE FOOTAGE</span>
    </div>

    {/* text block */}
    <div style={{position: "absolute", left: 44, bottom: 44, maxWidth: 780}}>
      <div style={{fontFamily: MONO, fontSize: 26, color: "#7fc4ff", letterSpacing: 5, marginBottom: 10}}>
        911 CALLER · 11:56 PM
      </div>
      <div style={{fontFamily: ANTON, fontSize: 108, lineHeight: 0.98, color: "#f6f6f4",
        textShadow: "0 4px 26px rgba(0,0,0,0.9)"}}>
        “THERE’S A <span style={{color: RED}}>SHOOTOUT</span>
      </div>
      <div style={{fontFamily: ANTON, fontSize: 108, lineHeight: 1.02, color: "#f6f6f4",
        textShadow: "0 4px 26px rgba(0,0,0,0.9)"}}>
        RIGHT NOW.”
      </div>
      <div style={{display: "flex", alignItems: "center", gap: 14, marginTop: 16}}>
        <div style={{width: 64, height: 5, background: RED}} />
        <span style={{fontFamily: MONO, fontSize: 24, color: "#cfd3d8", letterSpacing: 3}}>SAN DIEGO · BODYCAM</span>
        <div style={{width: 64, height: 5, background: BLUE}} />
      </div>
    </div>

    {/* brand mark */}
    <div style={{position: "absolute", right: 30, bottom: 26}}>
      <MagLens size={128} spin={22} glow={1} monogram="DR" tilt={-8} />
    </div>
  </AbsoluteFill>
);
