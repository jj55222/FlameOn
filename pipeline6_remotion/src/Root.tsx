import React from "react";
import {Composition} from "remotion";
import {Cut} from "./Cut";
import {MotifDemo} from "./motifs/MotifDemo";
import type {CutProps} from "./types";

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
  <Composition
    id="MotifDemo"
    component={MotifDemo}
    fps={30}
    width={1920}
    height={1080}
    durationInFrames={34 * 30}
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
