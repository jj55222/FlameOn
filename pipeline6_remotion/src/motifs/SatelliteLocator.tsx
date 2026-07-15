import React from "react";
import {AbsoluteFill, Img, interpolate, staticFile, useCurrentFrame, useVideoConfig} from "remotion";
import {Build, TypeOn} from "./TypeOn";
import {C, F, kicker, sourceLine} from "./theme";
import {UnitDot, UnitDots} from "./UnitDots";

export type SatelliteLocatorProps = {
  /** zoom ladder from fetch_aerial.py, wide->tight, under public/ */
  levels: Array<{file: string; half_width_m: number}>;
  dateTime: string;          // "DEC 07 2023 · 23:23"
  place: string;             // "RALPHS · 10675 4S COMMONS DR"
  coords?: string;           // "32.9613N 117.1856W"
  person?: {photo?: string; name: string};   // optional pinned card
  illustrated?: boolean;     // SolvedFiles night-illustration treatment
  holdSec?: number;
  /** motion-graphic unit dots shown once the zoom lands (percent coords on the tight frame) */
  dots?: UnitDot[];
  sharpen?: boolean;         // unsharp-mask on the tight level (default true)
};

/** Motifs 5+6 — real-address satellite zoom (public-domain NAIP), optional
 * illustrated night treatment. Zooms wide->tight through the ladder, then the
 * red pin drops and the person card fades in over the property. */
export const SatelliteLocator: React.FC<SatelliteLocatorProps> = (p) => {
  const frame = useCurrentFrame();
  const {fps, width, height} = useVideoConfig();
  const t = frame / fps;
  const perLevel = 2.2;
  const total = p.levels.length * perLevel;

  const filt = p.illustrated
    ? "grayscale(0.9) brightness(0.5) contrast(1.7) sepia(0.25) hue-rotate(175deg) saturate(1.6)"
    : "brightness(0.92) contrast(1.05)";

  return (
    <AbsoluteFill style={{backgroundColor: C.bg, overflow: "hidden"}}>
      {/* unsharp-mask kernel for the supersampled tight level */}
      <svg width={0} height={0} style={{position: "absolute"}}>
        <defs>
          <filter id="flameon-sharpen">
            <feConvolveMatrix order="3" kernelMatrix="0 -0.55 0 -0.55 3.2 -0.55 0 -0.55 0" preserveAlpha="true" />
          </filter>
        </defs>
      </svg>
      {p.levels.map((lvl, i) => {
        const last = i === p.levels.length - 1;
        const a = i * perLevel;
        const b = a + perLevel;
        const local = interpolate(t, [a, b], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
        // each level scales 1 -> ratio to hand off seamlessly to the next; the tight
        // level lands at a modest 1.18 so we stay inside the supersampled resolution
        const ratio = !last ? p.levels[i].half_width_m / p.levels[i + 1].half_width_m : 1.18;
        const scale = 1 + (ratio - 1) * (1 - Math.pow(1 - local, 2));
        const visible = t >= a - 0.05 && (last || t < b + 0.05);
        const sharp = last && p.sharpen !== false ? " url(#flameon-sharpen)" : "";
        return visible ? (
          <AbsoluteFill key={lvl.file} style={{transform: `scale(${scale})`, transformOrigin: "50% 50%"}}>
            <Img src={staticFile(lvl.file)} style={{width, height, objectFit: "cover", filter: filt + sharp}} />
            {p.illustrated ? (
              <AbsoluteFill style={{background: "radial-gradient(ellipse at center, rgba(10,16,26,0) 45%, rgba(4,6,10,0.88) 100%)"}} />
            ) : null}
          </AbsoluteFill>
        ) : null;
      })}

      {/* unit dots once the zoom lands */}
      {p.dots && t >= total - 0.2 ? (
        <UnitDots dots={p.dots.map((d) => ({...d, startSec: (d.startSec ?? 0) + total}))} />
      ) : null}

      {/* header builds immediately */}
      <div style={{position: "absolute", top: 64, left: 0, right: 0, textAlign: "center"}}>
        <div style={{...kicker, fontSize: 24}}>
          <TypeOn text={p.dateTime} startSec={0.2} charSec={0.02} />
        </div>
        <div style={{fontFamily: F.headline, color: C.white, fontSize: 56, textTransform: "uppercase", textShadow: "0 2px 18px rgba(0,0,0,0.9)"}}>
          <TypeOn text={p.place} startSec={0.7} charSec={0.03} />
        </div>
        {p.coords ? <div style={{...sourceLine, marginTop: 6}}>{p.coords}</div> : null}
      </div>

      {/* pin drop at the end of the zoom */}
      <PinDrop startSec={total - 0.6} />

      {p.person ? (
        <Build startSec={total - 0.2} style={{position: "absolute", left: "50%", top: "18%", transform: "translateX(-50%)"}}>
          <div style={{background: C.card, border: `1px solid ${C.cardEdge}`, padding: 10, width: 260, boxShadow: "0 18px 60px rgba(0,0,0,0.7)"}}>
            {p.person.photo ? (
              <Img src={staticFile(p.person.photo)} style={{width: 240, height: 280, objectFit: "cover"}} />
            ) : (
              <div style={{width: 240, height: 280, background: "#222", display: "flex", alignItems: "center", justifyContent: "center", color: C.red, fontFamily: F.headline, fontSize: 90}}>?</div>
            )}
            <div style={{fontFamily: F.label, fontWeight: 800, color: C.white, fontSize: 26, padding: "10px 4px 4px"}}>
              <TypeOn text={p.person.name} startSec={total + 0.4} charSec={0.03} />
            </div>
          </div>
        </Build>
      ) : null}

      <div style={{position: "absolute", left: 72, bottom: 48, ...sourceLine}}>
        <span style={{color: C.red}}>▪ </span>IMAGERY — USGS NAIP · public domain · location from case records
      </div>
    </AbsoluteFill>
  );
};

const PinDrop: React.FC<{startSec: number}> = ({startSec}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const p = interpolate(frame / fps, [startSec, startSec + 0.5], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const bounce = 1 - Math.abs(Math.cos(p * Math.PI * 1.5)) * (1 - p);
  const y = -140 * (1 - p);
  return (
    <div style={{position: "absolute", left: "50%", top: "50%", transform: `translate(-50%, calc(-100% + ${y}px)) scale(${0.7 + 0.3 * bounce})`, opacity: p > 0 ? 1 : 0}}>
      <svg width={54} height={72} viewBox="0 0 24 32">
        <path d="M12 0C5.4 0 0 5.4 0 12c0 9 12 20 12 20s12-11 12-20C24 5.4 18.6 0 12 0z" fill={C.red} />
        <circle cx="12" cy="12" r="4.6" fill="#fff" />
      </svg>
    </div>
  );
};
