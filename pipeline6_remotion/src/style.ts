import {interpolate} from "remotion";

export const RED = "#e0241b";
export const BG = "#0a0a0b";
export const PANEL = "#15161a";
export const INK = "#ececec";
export const DIM = "#92949b";
export const COND = "'Impact','Haettenschweiler','Arial Narrow Bold',sans-serif";
export const MONO = "ui-monospace,'SF Mono',Menlo,monospace";

export const fadeIO = (frame: number, duration: number, edge = 10) =>
  interpolate(frame, [0, edge, Math.max(edge + 1, duration - edge), duration], [0, 1, 1, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });
