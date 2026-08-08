import React from "react";
import {AbsoluteFill, Img, interpolate, staticFile, useCurrentFrame, useVideoConfig} from "remotion";
import {C, F, sourceLine} from "./theme";

/** File-viewer chrome — wrap ANY bundle artifact so it reads as "the actual file". */
export const FileViewer: React.FC<{
  title?: string;               // "File Viewer"
  filename: string;             // "CAR DASHCAM — Kristil"
  width: number;
  children: React.ReactNode;
  style?: React.CSSProperties;
}> = ({title = "File Viewer", filename, width, children, style}) => (
  <div style={{width, background: "#111114", border: `1px solid ${C.cardEdge}`, boxShadow: "0 24px 70px rgba(0,0,0,0.75)", ...style}}>
    <div style={{display: "flex", justifyContent: "space-between", alignItems: "center", padding: "7px 12px", background: "#1a1a1e", borderBottom: `1px solid ${C.cardEdge}`}}>
      <span style={{fontFamily: F.mono, fontSize: 15, color: C.dim}}>{title}</span>
      <span style={{display: "flex", gap: 6}}>
        {["#3a3a40", "#3a3a40", C.red].map((c, i) => <span key={i} style={{width: 10, height: 10, borderRadius: 5, background: c}} />)}
      </span>
    </div>
    {children}
    <div style={{padding: "8px 12px", fontFamily: F.label, fontWeight: 800, fontSize: 17, color: C.white, textTransform: "uppercase", letterSpacing: "0.05em"}}>
      {filename}
    </div>
  </div>
);

/** Motif 11 — route trace over the aerial + evidence PiP. The cyan route draws
 * across the geolocated imagery while the cited footage plays in a FileViewer
 * window; trace progress and PiP are meant to share a timeline. */
export const RouteTracePiP: React.FC<{
  aerial: string;                       // staticFile aerial (from fetch_aerial.py)
  route: Array<[number, number]>;       // percent waypoints
  durSec?: number;
  pipImage?: string;                    // frame(s) of the cited footage
  pipLabel: string;                     // "CAR DASHCAM — Kristil"
  header?: string;
  illustratedNight?: boolean;
}> = ({aerial, route, durSec = 6, pipImage, pipLabel, header, illustratedNight = false}) => {
  const frame = useCurrentFrame();
  const {fps, width, height} = useVideoConfig();
  const p = interpolate(frame / fps, [0.6, 0.6 + durSec], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const pts = route.map(([x, y]) => [(x / 100) * width, (y / 100) * height] as [number, number]);
  // polyline up to progress p
  const segs = pts.length - 1;
  const f = p * segs;
  const i = Math.min(segs - 1, Math.floor(f));
  const local = f - i;
  const head: [number, number] = [pts[i][0] + (pts[i + 1][0] - pts[i][0]) * local, pts[i][1] + (pts[i + 1][1] - pts[i][1]) * local];
  const drawn = [...pts.slice(0, i + 1), head];
  return (
    <AbsoluteFill style={{background: C.bg}}>
      <Img src={staticFile(aerial)} style={{width, height, objectFit: "cover", filter: illustratedNight ? "grayscale(0.9) brightness(0.45) contrast(1.7) sepia(0.25) hue-rotate(175deg) saturate(1.5)" : "saturate(0.55) brightness(0.75) contrast(1.1)"}} />
      <svg width={width} height={height} style={{position: "absolute", inset: 0}}>
        <polyline points={drawn.map(([x, y]) => `${x},${y}`).join(" ")} fill="none" stroke={C.cyan} strokeWidth={6} strokeLinecap="round" style={{filter: `drop-shadow(0 0 10px ${C.cyan})`}} />
        <circle cx={head[0]} cy={head[1]} r={11} fill={C.cyan} style={{filter: `drop-shadow(0 0 14px ${C.cyan})`}} />
        <circle cx={pts[0][0]} cy={pts[0][1]} r={7} fill="#fff" opacity={0.8} />
      </svg>
      {pipImage ? (
        <FileViewer filename={pipLabel} width={430} style={{position: "absolute", left: 64, top: 64}}>
          <Img src={staticFile(pipImage)} style={{width: "100%", height: 250, objectFit: "cover"}} />
        </FileViewer>
      ) : null}
      {header ? (
        <div style={{position: "absolute", top: 52, left: 0, right: 0, textAlign: "center", fontFamily: F.headline, fontSize: 50, color: C.white, textTransform: "uppercase", textShadow: "0 2px 16px rgba(0,0,0,0.9)"}}>
          {header}
        </div>
      ) : null}
      <div style={{position: "absolute", left: 64, bottom: 44, ...sourceLine}}>
        <span style={{color: C.red}}>▪ </span>route per cited footage/CAD · imagery USGS NAIP (public domain)
      </div>
    </AbsoluteFill>
  );
};
