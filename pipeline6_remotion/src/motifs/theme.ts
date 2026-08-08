// FlameOn visual-language tokens — derived from the operator's reference packet
// (Dr.Insanity doc/map/board motifs, SolvedFiles aerial/title style, EWU caption voices).
// Fonts: SIL/open faces matching the cinematic-condensed reference; distress comes from
// texture overlays, not the font license.

export const C = {
  bg: "#0a0a0b",
  card: "#161618",
  cardEdge: "#26262a",
  white: "#f4f4f2",
  dim: "#9a9a96",
  red: "#d81f26",
  redDeep: "#8f1216",
  cyan: "#35c8e8",
  blue911: "#3fa9f5",
  paper: "#b9b7b2",
  ink: "#1c1a17",
  gold: "#d8c690",
};

export const F = {
  headline: "'Anton', 'Arial Narrow', 'Impact', sans-serif",
  label: "'Archivo', 'Helvetica Neue', sans-serif",
  mono: "'JetBrains Mono', 'Menlo', monospace",
  hand: "'Caveat', 'Bradley Hand', cursive",
};

// Global build-timing contract (operator directive: nothing appears as a finished
// card — everything types/wipes on over ~a second).
export const T = {
  typeCharSec: 0.035,   // per-character type-on
  buildSec: 1.0,        // standard element build
  staggerSec: 0.55,     // gap between element starts
  fps: 30,
};

export const kicker = {
  fontFamily: F.label,
  fontWeight: 800 as const,
  letterSpacing: "0.22em",
  textTransform: "uppercase" as const,
  color: C.red,
  fontSize: 26,
};

export const sourceLine = {
  fontFamily: F.mono,
  fontSize: 22,
  color: C.dim,
  letterSpacing: "0.06em",
};
