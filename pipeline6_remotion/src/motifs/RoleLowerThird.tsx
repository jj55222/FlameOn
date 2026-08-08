import React from "react";
import {Build, TypeOn} from "./TypeOn";
import {C, F} from "./theme";

/** Motif 3 — role tag + typed transcript quote, over footage. */
export const RoleLowerThird: React.FC<{
  role: string;           // "RESPONDING DEPUTY"
  quote?: string;         // transcript line, typed on beneath
  startSec?: number;
  left?: number;
  bottom?: number;
}> = ({role, quote, startSec = 0, left = 64, bottom = 96}) => (
  <div style={{position: "absolute", left, bottom, maxWidth: 900}}>
    <Build startSec={startSec} from="left">
      <div style={{display: "inline-block", background: "rgba(8,8,9,0.92)", borderTop: `5px solid ${C.red}`, padding: "10px 18px"}}>
        <div style={{fontFamily: F.headline, color: C.white, fontSize: 34, letterSpacing: "0.04em", textTransform: "uppercase"}}>
          {role}
        </div>
      </div>
    </Build>
    {quote ? (
      <div style={{marginTop: 10, fontFamily: F.label, fontStyle: "italic", fontWeight: 600, fontSize: 34, color: C.white, textShadow: "0 2px 12px rgba(0,0,0,0.95)"}}>
        <TypeOn text={`“${quote}”`} startSec={startSec + 0.6} charSec={0.022} />
      </div>
    ) : null}
  </div>
);
