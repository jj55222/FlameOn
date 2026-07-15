import React from "react";
import {AbsoluteFill} from "remotion";
import {Build, TypeOn} from "./TypeOn";
import {C, F, T, kicker, sourceLine} from "./theme";

export type DocQuoteProps = {
  kickerText: string;          // "WHAT THE DEPUTIES FOUND"
  headline: string;            // "DEPUTY MARVIN MORALES"
  docLabel: string;            // "INTERNAL AFFAIRS · INVESTIGATION REPORT"
  quote: string;               // body text from doc_extract
  redPhrases?: string[];       // key phrases to ignite red
  analysis?: string;           // optional red side-block
  source: string;              // "Internal Affairs Report · 2013PSB-0530 · p.14"
};

/** Motif 1 — TypeOn Doc-Quote (Dr.Insanity doc-highlight DNA).
 * Build order: kicker types → headline types → quote card rises, body types with
 * red phrases → analysis block slides in → source line fades. */
export const DocQuote: React.FC<DocQuoteProps> = (p) => {
  const t0 = 0.2;
  const t1 = t0 + 0.7;                                   // headline start
  const t2 = t1 + Math.min(1.4, p.headline.length * T.typeCharSec) + 0.4;
  const t3 = t2 + 0.9 + Math.min(3.2, p.quote.length * 0.012); // analysis after quote mostly in
  return (
    <AbsoluteFill style={{backgroundColor: C.bg, padding: "72px 96px", fontFamily: F.label}}>
      <div style={kicker}>
        <TypeOn text={p.kickerText} startSec={t0} charSec={0.02} />
      </div>
      <div style={{fontFamily: F.headline, color: C.white, fontSize: 92, letterSpacing: "0.01em", marginTop: 10, textTransform: "uppercase"}}>
        <TypeOn text={p.headline} startSec={t1} charSec={0.045} caret />
      </div>

      <div style={{display: "flex", gap: 36, marginTop: 54, alignItems: "flex-start"}}>
        <Build startSec={t2} style={{maxWidth: 980}}>
          <div style={{background: C.card, border: `1px solid ${C.cardEdge}`, padding: "34px 40px"}}>
            <div style={{...sourceLine, color: C.dim, marginBottom: 18}}>{p.docLabel}</div>
            <div style={{fontFamily: F.mono, color: C.white, fontSize: 30, lineHeight: 1.55}}>
              <TypeOn text={p.quote} startSec={t2 + 0.35} charSec={0.011} redPhrases={p.redPhrases ?? []} redColor={C.red} />
            </div>
          </div>
        </Build>
        {p.analysis ? (
          <Build startSec={t3} from="left" style={{maxWidth: 420}}>
            <div style={{borderLeft: `4px solid ${C.red}`, paddingLeft: 22}}>
              <div style={{...kicker, fontSize: 20, marginBottom: 10}}>ANALYSIS</div>
              <div style={{color: C.dim, fontSize: 24, lineHeight: 1.5}}>{p.analysis}</div>
            </div>
          </Build>
        ) : null}
      </div>

      <Build startSec={t3 + 0.8} from="none" style={{position: "absolute", left: 96, bottom: 56}}>
        <div style={sourceLine}>
          <span style={{color: C.red}}>▪ </span>SOURCE — {p.source}
        </div>
      </Build>
    </AbsoluteFill>
  );
};
