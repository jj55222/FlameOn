import React from "react";
import {AbsoluteFill, Audio, staticFile, useCurrentFrame, useVideoConfig} from "remotion";
import {useAudioData, visualizeAudioWaveform} from "@remotion/media-utils";
import {TypeOn} from "./TypeOn";
import {C, F} from "./theme";
import {BLUE} from "../brand/MagLens";

export type CaptionWord = {text: string; startSec: number; endSec: number};

/** Motif 4 (upgraded) — full 911 CARD with a waveform driven by the ACTUAL call
 * audio (visualizeAudioWaveform), the real audio playing, and captions that track
 * the spoken words. This is the operator's reference treatment for 911 segments. */
export const Full911Card: React.FC<{
  audioSrc: string;              // staticFile path to the real 911 audio
  location: string;             // "RESERVOIR DRIVE"
  quote?: string;               // the key line, big and blue
  source: string;               // "911 Call 3 · SDPD release · transcript checked against audio"
  words?: CaptionWord[];        // word-timed transcript for the karaoke caption
  playAudio?: boolean;          // default true — plays the real call
  audioOffsetSec?: number;      // trim into the call
}> = ({audioSrc, location, quote, source, words = [], playAudio = true, audioOffsetSec = 0}) => {
  const frame = useCurrentFrame();
  const {fps, width, height} = useVideoConfig();
  const audioData = useAudioData(staticFile(audioSrc));

  // waveform reactive to the actual audio at this frame
  const N = 96;
  const samples = audioData
    ? visualizeAudioWaveform({audioData, frame, fps, numberOfSamples: N, windowInSeconds: 0.15})
    : new Array(N).fill(0);
  const wfW = width * 0.72;
  const wfX = (width - wfW) / 2;
  const cy = height * 0.62;
  const step = wfW / (N - 1);
  const path = samples
    .map((a, i) => `${i === 0 ? "M" : "L"}${(wfX + i * step).toFixed(1)},${(cy - a * 150).toFixed(1)}`)
    .join(" ");
  const pathMirror = samples
    .map((a, i) => `${i === 0 ? "M" : "L"}${(wfX + i * step).toFixed(1)},${(cy + a * 150).toFixed(1)}`)
    .join(" ");

  const tSec = frame / fps;

  return (
    <AbsoluteFill style={{background: "radial-gradient(ellipse at center, #0c1826 0%, #060a10 75%)"}}>
      {playAudio ? <Audio src={staticFile(audioSrc)} startFrom={Math.round(audioOffsetSec * fps)} /> : null}

      {/* big 911 + location */}
      <div style={{position: "absolute", top: "18%", left: 0, right: 0, textAlign: "center"}}>
        <div style={{fontFamily: F.label, fontWeight: 300, fontSize: 150, color: C.white, letterSpacing: "0.04em", textShadow: `0 0 40px ${BLUE}88`}}>911</div>
        <div style={{fontFamily: F.mono, fontSize: 30, color: BLUE, letterSpacing: "0.22em", marginTop: 4}}>
          <TypeOn text={`${location} · ORIGINAL CALL AUDIO`} startSec={0.2} charSec={0.02} />
        </div>
      </div>

      {/* reactive waveform */}
      <svg width={width} height={height} style={{position: "absolute", inset: 0}}>
        <path d={path} fill="none" stroke={BLUE} strokeWidth={3} strokeLinecap="round" style={{filter: `drop-shadow(0 0 10px ${BLUE})`}} opacity={0.95} />
        <path d={pathMirror} fill="none" stroke={BLUE} strokeWidth={3} strokeLinecap="round" opacity={0.35} />
      </svg>

      {/* the key quote — big blue */}
      {quote ? (
        <div style={{position: "absolute", bottom: "20%", left: 0, right: 0, textAlign: "center", padding: "0 8%"}}>
          <div style={{fontFamily: F.label, fontWeight: 800, fontSize: 62, color: "#7fc4ff", textShadow: `0 2px 18px rgba(0,0,0,0.8)`}}>
            <TypeOn text={`“${quote}”`} startSec={0.6} charSec={0.028} />
          </div>
        </div>
      ) : null}

      {/* word-synced caption of the actual audio */}
      {words.length ? (
        <div style={{position: "absolute", bottom: "9%", left: 0, right: 0, textAlign: "center", padding: "0 10%"}}>
          <span style={{fontFamily: F.label, fontWeight: 600, fontSize: 40, lineHeight: 1.3}}>
            {words.map((w, i) => {
              const on = tSec >= w.startSec;
              const active = tSec >= w.startSec && tSec < w.endSec;
              return (
                <span key={i} style={{color: on ? "#ffffff" : "rgba(255,255,255,0.32)", textShadow: active ? `0 0 16px ${BLUE}` : undefined, transition: "none"}}>
                  {w.text}{" "}
                </span>
              );
            })}
          </span>
        </div>
      ) : null}

      {/* source line */}
      <div style={{position: "absolute", left: 64, bottom: 40, fontFamily: F.mono, fontSize: 22, color: "#6f8296", letterSpacing: "0.05em"}}>
        <span style={{color: BLUE}}>▪ </span>{source}
      </div>
    </AbsoluteFill>
  );
};
