import React from "react";
import {
  AbsoluteFill,
  Audio,
  Img,
  OffthreadVideo,
  Series,
  interpolate,
  staticFile,
  useCurrentFrame,
  useVideoConfig,
} from "remotion";
import {useAudioData, visualizeAudio} from "@remotion/media-utils";
import {BG, COND, DIM, INK, MONO, PANEL, RED, fadeIO} from "./style";
import type {Caption, CutEvent, CutProps, DocEvent, MapEvent, MediaEvent} from "./types";

const RichText: React.FC<{text: string}> = ({text}) => (
  <>
    {text.split(/\*\*/).map((segment, index) =>
      index % 2 === 1 ? (
        <span key={index} style={{color: RED, fontWeight: 700}}>{segment}</span>
      ) : (
        <span key={index}>{segment}</span>
      ),
    )}
  </>
);

export const NarrationBand: React.FC<{text: string; durSec: number; voDur?: number}> = ({text, durSec, voDur}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  if (!text) return null;
  const words = text.trim().split(/\s+/).length;
  const holdSec = Math.min(voDur ? voDur + 0.5 : Math.max(4.5, words / 3.2), Math.max(5, durSec - 0.35));
  const hold = holdSec * fps;
  // ~0.75s build (22f) in and out per operator polish spec
  const opacity = interpolate(frame, [0, 22, Math.max(24, hold - 18), hold], [0, 1, 1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
  const rise = interpolate(frame, [0, 22], [16, 0], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const ruleW = interpolate(frame, [4, 26], [0, 46], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  if (opacity < 0.01) return null;
  return (
    <div style={{
      position: "absolute", top: 0, left: 0, right: 0, opacity, transform: `translateY(${-rise}px)`,
      background: "linear-gradient(180deg, rgba(6,6,8,0.94) 0%, rgba(6,6,8,0.82) 72%, rgba(6,6,8,0) 100%)",
      padding: "24px 7% 46px",
    }}>
      <div style={{width: ruleW, height: 4, background: RED, marginBottom: 12}} />
      <div style={{color: INK, fontFamily: "Georgia, 'Times New Roman', serif", fontSize: 28,
        lineHeight: 1.4, maxWidth: "88%", textShadow: "0 1px 3px rgba(0,0,0,.9)"}}>
        <RichText text={text} />
      </div>
    </div>
  );
};

export const CaptionTrack: React.FC<{captions: Caption[]; offset?: number; large?: boolean}> = ({captions, offset = 0, large}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const time = frame / fps - offset;
  const current = captions.find((caption) => time >= caption.start && time < caption.end);
  if (!current) return null;
  const words = current.text.split(/\s+/).filter(Boolean);
  const weights = words.map((word) => word.length + 2);
  const total = Math.max(1, weights.reduce((a, b) => a + b, 0));
  const span = Math.max(0.1, current.end - current.start);
  let cursor = current.start;
  const starts = weights.map((weight) => {
    const start = cursor;
    cursor += (weight / total) * span;
    return start;
  });
  const active = starts.filter((start) => time >= start).length - 1;
  // gentle build-in for each caption box (~0.35s)
  const inP = interpolate(time - current.start, [0, 0.35], [0, 1], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  return (
    <div style={{position: "absolute", left: 0, right: 0, bottom: large ? "26%" : 112,
      display: "flex", justifyContent: "center", pointerEvents: "none", opacity: inP,
      transform: `translateY(${(1 - inP) * 8}px)`}}>
      <div style={{background: "rgba(0,0,0,0.78)", borderRadius: 6, padding: "10px 22px",
        fontSize: large ? 45 : 38, lineHeight: 1.28, maxWidth: "84%", color: INK,
        fontFamily: "'Helvetica Neue',Arial,sans-serif", fontWeight: 650, textAlign: "center",
        boxShadow: "0 4px 20px rgba(0,0,0,.28)"}}>
        {words.map((word, index) => (
          <span key={`${index}-${word}`} style={{
            color: index <= active ? INK : "rgba(236,236,236,0.42)",
            textShadow: index === active ? "0 0 14px rgba(255,255,255,0.38)" : undefined,
          }}>{word}{index < words.length - 1 ? " " : ""}</span>
        ))}
      </div>
    </div>
  );
};

export const LowerThird: React.FC<{label: string}> = ({label}) => {
  const frame = useCurrentFrame();
  if (!label) return null;
  // ~0.75s build (22f): slide + fade + rule wipe
  const x = interpolate(frame, [0, 22], [-46, 0], {extrapolateRight: "clamp"});
  const opacity = interpolate(frame, [0, 22], [0, 1], {extrapolateRight: "clamp"});
  const barH = interpolate(frame, [2, 24], [0, 100], {extrapolateLeft: "clamp", extrapolateRight: "clamp"});
  const [main, credit] = label.split("  -  ").length > 1 ? label.split("  -  ") : label.split("  ·  ");
  return (
    <div style={{position: "absolute", left: 30, bottom: 28, transform: `translateX(${x}px)`,
      opacity, display: "flex", alignItems: "stretch", maxWidth: "88%"}}>
      <div style={{width: 6, background: RED, flex: "0 0 auto", height: `${barH}%`, alignSelf: "flex-end"}} />
      <div style={{background: "rgba(10,10,11,0.9)", padding: "8px 16px 9px", minWidth: 0}}>
        <div style={{color: INK, fontFamily: COND, fontSize: 25, letterSpacing: 0,
          textTransform: "uppercase", whiteSpace: "normal"}}>{main}</div>
        {credit ? <div style={{color: DIM, fontFamily: MONO, fontSize: 13, marginTop: 3}}>{credit}</div> : null}
      </div>
    </div>
  );
};

const voice = (props: CutProps, event: {voFile?: string; voDur?: number}) =>
  props.audio.voEnabled && event.voFile ? event : null;

const duck = (duration: number, fps: number, floor: number) => (frame: number) =>
  interpolate(frame, [0, Math.round(duration * fps), Math.round(duration * fps) + fps], [floor, floor, 1], {
    extrapolateLeft: "clamp", extrapolateRight: "clamp",
  });

const Waveform: React.FC<{src: string}> = ({src}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const data = useAudioData(staticFile(src));
  const indexes = [0, 1, 2, 3, 4, 5, 6];
  let amplitudes: number[] | null = null;
  if (data) {
    const waveform = visualizeAudio({fps, frame, audioData: data, numberOfSamples: 16});
    amplitudes = [2, 5, 7, 9, 7, 5, 2].map((bin) => Math.min(1, Math.pow((waveform[bin] ?? 0) * 5, 0.6)));
  }
  return (
    <div style={{display: "flex", gap: 10, alignItems: "flex-end", height: 122}}>
      {indexes.map((index) => {
        const height = amplitudes ? 12 + 98 * amplitudes[index] : 22 + 9 * Math.abs(Math.sin(frame / (9 + index) + index));
        return <div key={index} style={{width: 14, height, background: index === 3 ? RED : "#3a3b41"}} />;
      })}
    </div>
  );
};

// Operator spec: muted bodycam lead-in (Axon pre-event buffer) shows a live countdown,
// never dead silence. Driven by event.audioResumesInSec (set by the adapter on silent leads).
const AudioResumeCountdown: React.FC<{resumesInSec: number}> = ({resumesInSec}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const remaining = Math.max(0, resumesInSec - frame / fps);
  if (remaining <= 0.1) return null;
  const secs = Math.ceil(remaining);
  return (
    <div style={{position: "absolute", left: 0, right: 0, top: "44%", textAlign: "center", pointerEvents: "none"}}>
      <div style={{display: "inline-flex", alignItems: "center", gap: 14, background: "rgba(8,8,10,0.72)",
        border: `1px solid ${RED}`, borderRadius: 8, padding: "12px 22px"}}>
        <div style={{width: 11, height: 11, borderRadius: 11, background: RED, opacity: 0.5 + 0.5 * Math.sin(frame / 5)}} />
        <span style={{fontFamily: MONO, fontSize: 26, color: INK, letterSpacing: 2}}>
          AUDIO RESUMES IN {secs}s
        </span>
      </div>
    </div>
  );
};

const ClipEvent: React.FC<{event: MediaEvent; props: CutProps}> = ({event, props}) => {
  const {fps} = useVideoConfig();
  const vo = voice(props, event);
  return (
    <AbsoluteFill style={{background: "#000"}}>
      <OffthreadVideo src={staticFile(event.file)} style={{width: "100%", height: "100%", objectFit: "contain"}}
        volume={vo ? duck(event.voDur ?? 0, fps, props.audio.duckTo) : undefined} />
      {vo ? <Audio src={staticFile(vo.voFile!)} /> : null}
      {event.audioResumesInSec ? <AudioResumeCountdown resumesInSec={event.audioResumesInSec} /> : null}
      <CaptionTrack captions={event.captions} offset={event.capOffset} />
      <NarrationBand text={event.narrationTop} durSec={event.durSec} voDur={vo?.voDur} />
      <LowerThird label={event.lowerThird} />
    </AbsoluteFill>
  );
};

const CALL_BLUE = "#2456f0";

// Blue line waveform reactive to the actual call audio.
const LineWaveform: React.FC<{src: string; width: number; color: string}> = ({src, width, color}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const data = useAudioData(staticFile(src));
  const N = 90;
  const samples = data ? visualizeAudio({fps, frame, audioData: data, numberOfSamples: N * 2}) : null;
  const w = width * 0.72;
  const step = w / (N - 1);
  const amp = (i: number) => (samples ? Math.min(1, Math.pow((samples[i] ?? 0) * 6, 0.6)) * 70 : 2 * Math.sin(frame / 6 + i));
  const up = Array.from({length: N}, (_, i) => `${i === 0 ? "M" : "L"}${(i * step).toFixed(1)},${(80 - amp(i)).toFixed(1)}`).join(" ");
  const dn = Array.from({length: N}, (_, i) => `${i === 0 ? "M" : "L"}${(i * step).toFixed(1)},${(80 + amp(i)).toFixed(1)}`).join(" ");
  return (
    <svg width={w} height={160} style={{overflow: "visible"}}>
      <path d={up} fill="none" stroke={color} strokeWidth={3} strokeLinecap="round" style={{filter: `drop-shadow(0 0 9px ${color})`}} />
      <path d={dn} fill="none" stroke={color} strokeWidth={3} strokeLinecap="round" opacity={0.35} />
    </svg>
  );
};

const AudioEvent: React.FC<{event: MediaEvent; props: CutProps}> = ({event, props}) => {
  const frame = useCurrentFrame();
  const {fps, width} = useVideoConfig();
  const vo = voice(props, event);
  const [main, credit] = event.lowerThird.split("  ·  ");
  const is911 = /911/i.test(event.file) || /911/.test(event.lowerThird);
  const isInterview = /interview|interrogat/i.test(event.file) || /interview|interrogat/i.test(event.lowerThird);
  const audioEl = <Audio src={staticFile(event.file)} volume={vo ? duck(event.voDur ?? 0, fps, props.audio.duckTo) : undefined} />;

  // Blue-line audio card (operator spec: ONE style for 911 + interview audio).
  if (is911 || isInterview) {
    const bignum = is911 ? "911" : "INTERVIEW";
    const numSize = is911 ? 150 : 92;
    const sub = is911
      ? `${(main || "911 CALL").replace(/^911[\s·-]*/i, "").toUpperCase() || "CALL"} · ORIGINAL CALL AUDIO`
      : `${(main || "RECORDED INTERVIEW").replace(/interview[\s·-]*/i, "").toUpperCase() || "RECORDED"} · INTERVIEW AUDIO`;
    return (
      <AbsoluteFill style={{background: "radial-gradient(ellipse at center, #0c1826 0%, #060a10 75%)", justifyContent: "center", alignItems: "center"}}>
        {audioEl}
        {vo ? <Audio src={staticFile(vo.voFile!)} /> : null}
        <div style={{position: "absolute", top: "18%", textAlign: "center"}}>
          <div style={{fontFamily: MONO, fontSize: numSize, fontWeight: 300, color: "#f4f4f2", letterSpacing: 6, textShadow: `0 0 40px ${CALL_BLUE}88`}}>{bignum}</div>
          <div style={{fontFamily: MONO, fontSize: 28, color: CALL_BLUE, letterSpacing: 6, marginTop: 8}}>{sub}</div>
        </div>
        <LineWaveform src={event.file} width={width} color={CALL_BLUE} />
        <div style={{position: "absolute", left: 60, bottom: 40, fontFamily: MONO, fontSize: 20, color: "#6f8296"}}>
          <span style={{color: CALL_BLUE}}>▪ </span>{credit || event.creditLine}
        </div>
        <CaptionTrack captions={event.captions} offset={event.capOffset} />
      </AbsoluteFill>
    );
  }

  return (
    <AbsoluteFill style={{background: BG, justifyContent: "center", alignItems: "center"}}>
      {audioEl}
      {vo ? <Audio src={staticFile(vo.voFile!)} /> : null}
      <div style={{position: "absolute", top: 40, right: 46, display: "flex", alignItems: "center", gap: 12}}>
        <div style={{width: 12, height: 12, borderRadius: 12, background: RED,
          opacity: 0.55 + 0.45 * Math.sin(frame / 9)}} />
        <div style={{color: DIM, fontFamily: MONO, fontSize: 16, letterSpacing: 3}}>EVIDENCE AUDIO - RECORDED</div>
      </div>
      <div style={{display: "flex", flexDirection: "column", alignItems: "center", marginTop: -70}}>
        <div style={{color: INK, fontFamily: COND, fontSize: 44, marginBottom: 28,
          textTransform: "uppercase", textAlign: "center", maxWidth: 1000}}>{main || "Audio evidence"}</div>
        <Waveform src={event.file} />
        <div style={{color: DIM, fontFamily: MONO, fontSize: 14, marginTop: 24}}>{credit || event.creditLine}</div>
      </div>
      <CaptionTrack captions={event.captions} offset={event.capOffset} />
      <NarrationBand text={event.narrationTop} durSec={event.durSec} voDur={vo?.voDur} />
    </AbsoluteFill>
  );
};

export const DocCallout: React.FC<{event: DocEvent; props: CutProps}> = ({event, props}) => {
  const frame = useCurrentFrame();
  const {fps, durationInFrames} = useVideoConfig();
  const progress = frame / Math.max(1, durationInFrames);
  const cx = event.rects.length ? event.rects.reduce((sum, rect) => sum + rect.x + rect.w / 2, 0) / event.rects.length : 0.5;
  const cy = event.rects.length ? event.rects.reduce((sum, rect) => sum + rect.y + rect.h / 2, 0) / event.rects.length : 0.5;
  const scale = 1.03 + progress * 0.12;
  const vo = voice(props, event);
  return (
    <AbsoluteFill style={{background: BG, justifyContent: "center", alignItems: "center", opacity: fadeIO(frame, durationInFrames, 8)}}>
      {vo ? <Audio src={staticFile(vo.voFile!)} /> : null}
      <div style={{position: "absolute", top: 40, right: 46, color: DIM, fontFamily: MONO,
        fontSize: 16, letterSpacing: 3}}>CASE FILE</div>
      <div style={{transform: `scale(${scale}) translate(${(0.5 - cx) * 210 * progress}px, ${(0.5 - cy) * 250 * progress}px)`}}>
        <div style={{position: "relative", width: 1060, maxHeight: 560, overflow: "hidden",
          boxShadow: "0 18px 70px rgba(0,0,0,.65)", border: "1px solid #26272c"}}>
          <Img src={staticFile(event.img)} style={{width: "100%", display: "block"}} />
          {event.rects.map((rect, index) => {
            const start = (0.8 + index * 0.55) * fps;
            const grow = interpolate(frame, [start, start + 0.45 * fps], [0, 1], {
              extrapolateLeft: "clamp", extrapolateRight: "clamp",
            });
            return <div key={index} style={{position: "absolute", left: `${rect.x * 100}%`, top: `${rect.y * 100}%`,
              width: `${rect.w * 100}%`, height: `${rect.h * 100}%`, background: "rgba(255,225,0,.30)",
              border: "2px solid rgba(255,180,0,.95)", transform: `scaleX(${grow})`, transformOrigin: "left center"}} />;
          })}
        </div>
      </div>
      <NarrationBand text={event.narrationTop} durSec={event.durSec} voDur={vo?.voDur} />
      <LowerThird label={event.lowerThird} />
    </AbsoluteFill>
  );
};

export const TacticalMap: React.FC<{event: MapEvent; props: CutProps}> = ({event, props}) => {
  const frame = useCurrentFrame();
  const {durationInFrames} = useVideoConfig();
  const progress = frame / Math.max(1, durationInFrames);
  const vo = voice(props, event);
  return (
    <AbsoluteFill style={{background: BG, justifyContent: "center", alignItems: "center"}}>
      {vo ? <Audio src={staticFile(vo.voFile!)} /> : null}
      {event.background ? <Img src={staticFile(event.background)} style={{width: "100%", height: "100%", objectFit: "cover", opacity: 0.28}} /> : null}
      <div style={{position: "absolute", inset: 42, border: "1px solid #32343b",
        transform: `scale(${1 + 0.08 * progress})`, background: "rgba(12,13,16,.72)"}}>
        <div style={{position: "absolute", top: 20, left: 24, color: INK, fontFamily: COND, fontSize: 36}}>{event.title}</div>
        <svg width="100%" height="100%" style={{position: "absolute", inset: 0}}>
          {event.lines.map((line, index) => <line key={index} x1={`${line.from[0] * 100}%`} y1={`${line.from[1] * 100}%`}
            x2={`${line.to[0] * 100}%`} y2={`${line.to[1] * 100}%`} stroke={RED} strokeWidth={3} strokeDasharray="10 8" />)}
        </svg>
        {event.points.map((point, index) => <div key={index} style={{position: "absolute", left: `${point.x * 100}%`, top: `${point.y * 100}%`,
          transform: "translate(-50%,-50%)", border: `2px solid ${point.tone === "hot" ? RED : "#777b86"}`,
          background: point.tone === "hot" ? "rgba(224,36,27,.16)" : "rgba(35,37,43,.9)", padding: "10px 14px",
          color: point.tone === "hot" ? RED : INK, fontFamily: MONO, fontSize: 14}}>{point.label}</div>)}
      </div>
      <NarrationBand text={event.narrationTop} durSec={event.durSec} voDur={vo?.voDur} />
      <LowerThird label={event.lowerThird} />
    </AbsoluteFill>
  );
};

const Card: React.FC<{event: Extract<CutEvent, {type: "card"}>; agency: string}> = ({event, agency}) => {
  const frame = useCurrentFrame();
  const {durationInFrames} = useVideoConfig();
  const title = event.cardKind === "title";
  const titleSize = event.title.length > 260 ? 36 : event.title.length > 190 ? 40 : 46;
  return (
    <AbsoluteFill style={{background: BG, justifyContent: "center", padding: "0 9%", opacity: fadeIO(frame, durationInFrames, 9)}}>
      <div style={{color: RED, fontFamily: MONO, fontSize: title ? 19 : 16, letterSpacing: 4,
        marginBottom: 22, textTransform: "uppercase"}}>{title ? `${agency} - DOCUMENTARY CUT` : event.subtitle || agency}</div>
      <div style={{color: INK, fontFamily: COND, fontSize: title ? titleSize : 52, lineHeight: 1.16,
        textTransform: title ? "none" : "uppercase", maxWidth: "94%"}}>{event.title}</div>
      {title && event.subtitle ? <div style={{color: DIM, fontFamily: MONO, fontSize: 17, marginTop: 26}}>{event.subtitle}</div> : null}
    </AbsoluteFill>
  );
};

const NarrationCard: React.FC<{event: Extract<CutEvent, {type: "narration"}>; props: CutProps}> = ({event, props}) => {
  const frame = useCurrentFrame();
  const {durationInFrames} = useVideoConfig();
  const vo = voice(props, event);
  return (
    <AbsoluteFill style={{background: PANEL, justifyContent: "center", padding: "0 11%", opacity: fadeIO(frame, durationInFrames, 12)}}>
      {vo ? <Audio src={staticFile(vo.voFile!)} /> : null}
      <div style={{width: 60, height: 5, background: RED, marginBottom: 26}} />
      <div style={{color: INK, fontFamily: "Georgia, serif", fontSize: 30, lineHeight: 1.55, maxWidth: "92%"}}><RichText text={event.text} /></div>
      {event.footer ? <div style={{color: DIM, fontFamily: MONO, fontSize: 15, marginTop: 30}}>{event.footer}</div> : null}
    </AbsoluteFill>
  );
};

const EventView: React.FC<{event: CutEvent; props: CutProps}> = ({event, props}) => {
  if (event.type === "card") return <Card event={event} agency={props.agency} />;
  if (event.type === "narration") return <NarrationCard event={event} props={props} />;
  if (event.type === "doc") return <DocCallout event={event} props={props} />;
  if (event.type === "map") return <TacticalMap event={event} props={props} />;
  if (event.type === "audio") return <AudioEvent event={event} props={props} />;
  return <ClipEvent event={event} props={props} />;
};

export const Cut: React.FC<CutProps> = (props) => {
  const {fps} = useVideoConfig();
  return (
    <AbsoluteFill style={{background: "#000"}}>
      <Series>
        {props.events.map((event) => (
          <Series.Sequence key={event.eventId} durationInFrames={Math.max(1, Math.round(event.durSec * fps))}>
            <EventView event={event} props={props} />
          </Series.Sequence>
        ))}
      </Series>
    </AbsoluteFill>
  );
};
