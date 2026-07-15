import React from "react";
import {Composition, Sequence, AbsoluteFill} from "remotion";
import {Cut} from "./Cut";
import {HypeSpinner} from "./brand/HypeSpinner";
import {LikeSubscribe} from "./brand/LikeSubscribe";
import {LogoLockup} from "./brand/Logo";
import {MagLens} from "./brand/MagLens";
import {MotifDemo} from "./motifs/MotifDemo";
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
