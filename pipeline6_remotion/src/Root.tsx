import React from "react";
import {Composition, Sequence, AbsoluteFill} from "remotion";
import {Cut} from "./Cut";
import {HypeSpinner} from "./brand/HypeSpinner";
import {LikeSubscribe} from "./brand/LikeSubscribe";
import {LogoLockup} from "./brand/Logo";
import {MagLens} from "./brand/MagLens";
import {MotifDemo} from "./motifs/MotifDemo";
import {Full911Card} from "./motifs/Full911Card";
import words911 from "../public/demo911/words.json";
import type {CutProps} from "./types";


const LensMark: React.FC<{spin?: boolean}> = ({spin = false}) => {
  return (
    <AbsoluteFill style={{alignItems: "center", justifyContent: "center"}}>
      <LensInner spin={spin} />
    </AbsoluteFill>
  );
};
import {useCurrentFrame, useVideoConfig} from "remotion";
const LensInner: React.FC<{spin: boolean}> = ({spin}) => {
  const f = useCurrentFrame();
  const {fps} = useVideoConfig();
  return <MagLens size={760} spin={spin ? (f / fps) * 120 : 22} glow={1} monogram="DR" tilt={-8} />;
};


/** YouTube channel banner 2560x1440 — critical content inside the 1546x423 center
 * safe area (all-device visible); oversized arcs bleed outside for desktop/TV. */
const Banner: React.FC = () => (
  <AbsoluteFill style={{background: "#060607", alignItems: "center", justifyContent: "center"}}>
    <div style={{position: "absolute", left: -340, top: "50%", transform: "translateY(-50%)", opacity: 0.5}}>
      <MagLens size={900} spin={205} glow={1} monogram="" tilt={18} />
    </div>
    <div style={{position: "absolute", right: -340, top: "50%", transform: "translateY(-50%)", opacity: 0.5}}>
      <MagLens size={900} spin={65} glow={1} monogram="" tilt={-24} />
    </div>
    <div style={{display: "flex", alignItems: "center", gap: 40}}>
      <span style={{fontFamily: "'Anton','Arial Narrow','Impact',sans-serif", color: "#f4f4f2", fontSize: 150, letterSpacing: "0.02em"}}>TRUE CRIME</span>
      <MagLens size={230} spin={22} glow={1} monogram="DR" tilt={-8} />
    </div>
    <div style={{display: "flex", alignItems: "center", gap: 20, marginTop: 6}}>
      <div style={{width: 120, height: 3, background: "#d81f26"}} />
      <div style={{fontFamily: "'Archivo','Helvetica Neue',sans-serif", fontWeight: 800, letterSpacing: "0.42em", color: "#f4f4f2", fontSize: 22}}>EVERY&nbsp;CASE&nbsp;ON&nbsp;RECORD</div>
      <div style={{width: 120, height: 3, background: "#2456f0"}} />
    </div>
  </AbsoluteFill>
);


const Demo911: React.FC = () => (
  <Full911Card audioSrc="demo911/call.wav" location="4S COMMONS DRIVE"
    quote="There's a shootout right now." source="911 Call 1 · SDPD release · transcript checked against audio"
    words={words911 as any} />
);

const BrandReel: React.FC = () => (
  <AbsoluteFill>
    <Sequence from={0} durationInFrames={135}><HypeSpinner /></Sequence>
    <Sequence from={135} durationInFrames={90}><LogoLockup /></Sequence>
    <Sequence from={225} durationInFrames={195}><LikeSubscribe /></Sequence>
  </AbsoluteFill>
);

const defaults: CutProps = {
  schemaVersion: "flameon.remotion.v1",
  caseId: "case",
  agency: "Releasing Agency",
  fps: 30,
  width: 1280,
  height: 720,
  totalSec: 1,
  audio: {voEnabled: false, duckTo: 0.24},
  beats: [],
  events: [{type: "card", eventId: "placeholder", cardKind: "title", title: "FlameOn", subtitle: "", durSec: 1}],
  docCallouts: [],
  degradations: [],
  provenance: {},
};

export const RemotionRoot: React.FC = () => (
  <>
  <Composition id="Demo911" component={Demo911} fps={30} width={1920} height={1080} durationInFrames={450} />
  <Composition id="Banner" component={Banner} fps={30} width={2560} height={1440} durationInFrames={30} />
  <Composition id="LogoSquare800" component={LensMark} fps={30} width={800} height={800} durationInFrames={30} defaultProps={{spin: false}} />
  <Composition id="HypeOpener" component={HypeSpinner} fps={30} width={1920} height={1080} durationInFrames={135} />
  <Composition id="BrandReel" component={BrandReel} fps={30} width={1920} height={1080} durationInFrames={420} />
  <Composition id="LogoAlpha" component={LogoLockup} fps={30} width={1920} height={1080} durationInFrames={30} defaultProps={{animate: false, transparent: true}} />
  <Composition id="LensAvatar" component={LensMark} fps={30} width={1024} height={1024} durationInFrames={30} defaultProps={{spin: false}} />
  <Composition id="LensLoop" component={LensMark} fps={30} width={1024} height={1024} durationInFrames={120} defaultProps={{spin: true}} />
  <Composition id="LikeSubscribeAlpha" component={LikeSubscribe} fps={30} width={1920} height={1080} durationInFrames={195} defaultProps={{transparent: true}} />
  <Composition id="HypeAlpha" component={HypeSpinner} fps={30} width={1920} height={1080} durationInFrames={135} defaultProps={{transparent: true}} />
  <Composition id="LogoStill" component={LogoLockup} fps={30} width={1920} height={1080} durationInFrames={30} defaultProps={{animate: false}} />
  <Composition
    id="MotifDemo"
    component={MotifDemo}
    fps={30}
    width={1920}
    height={1080}
    durationInFrames={103 * 30}
    defaultProps={{hasAerial: true}}
  />
  <Composition
    id="Cut"
    component={Cut}
    fps={30}
    width={1280}
    height={720}
    durationInFrames={30}
    defaultProps={defaults}
    calculateMetadata={({props}) => {
      const p = props as CutProps;
      return {
        fps: p.fps,
        width: p.width,
        height: p.height,
        durationInFrames: p.events.reduce((sum, event) => sum + Math.max(1, Math.round(event.durSec * p.fps)), 0),
      };
    }}
  />
  </>
);
