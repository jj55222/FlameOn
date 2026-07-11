export type Caption = {start: number; end: number; text: string};
export type HighlightRect = {x: number; y: number; w: number; h: number};

type Voice = {voFile?: string; voDur?: number};
type Overlay = Voice & {durSec: number; narrationTop: string; lowerThird: string};

export type CardEvent = {
  type: "card";
  eventId: string;
  cardKind: string;
  title: string;
  subtitle: string;
  durSec: number;
};

export type NarrationEvent = Voice & {
  type: "narration";
  eventId: string;
  text: string;
  footer: string;
  durSec: number;
};

export type MediaEvent = Overlay & {
  type: "clip" | "audio";
  eventId: string;
  file: string;
  captions: Caption[];
  capOffset: number;
  creditLine: string;
  sourceLabel: string;
};

export type DocEvent = Overlay & {
  type: "doc";
  eventId: string;
  img: string;
  imgW: number;
  imgH: number;
  rects: HighlightRect[];
  source: string;
};

export type MapPoint = {x: number; y: number; label: string; tone?: "hot" | "cool"};
export type MapLine = {from: [number, number]; to: [number, number]; label?: string};
export type MapEvent = Overlay & {
  type: "map";
  eventId: string;
  title: string;
  background?: string;
  points: MapPoint[];
  lines: MapLine[];
};

export type CutEvent = CardEvent | NarrationEvent | MediaEvent | DocEvent | MapEvent;

export type CutProps = {
  schemaVersion: string;
  caseId: string;
  agency: string;
  fps: number;
  width: number;
  height: number;
  totalSec: number;
  audio: {voEnabled: boolean; duckTo: number};
  beats: Array<{beatId: string; sourceFile: string; inSec: number; outSec: number; lowerThird: string; narration: string}>;
  events: CutEvent[];
  docCallouts: Array<{id: string; img: string; imgW: number; imgH: number; rects: HighlightRect[]}>;
  degradations: Array<{code: string; detail: string; eventId?: string}>;
  provenance: Record<string, string>;
};
