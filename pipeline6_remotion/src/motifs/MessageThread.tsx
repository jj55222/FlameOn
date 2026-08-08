import React from "react";
import {AbsoluteFill, useCurrentFrame, useVideoConfig} from "remotion";
import {Build} from "./TypeOn";
import {C, F} from "./theme";

export type Bubble = {text: string; from: "them" | "me"; appearSec: number};

/** Motif 10 — in-hand message thread (Dr.Insanity opener DNA). Bubbles pop in
 * sequence with a typing indicator; text VERBATIM from case records (citations
 * enforced upstream); honesty strip on by default for recreations. */
export const MessageThread: React.FC<{
  header: string;                // "Unknown Number" / a redacted name
  bubbles: Bubble[];
  illustrative?: boolean;
  clock?: string;                // "1:42"
}> = ({header, bubbles, illustrative = true, clock = "1:42"}) => {
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();
  const t = frame / fps;
  const nextIdx = bubbles.findIndex((b) => t < b.appearSec);
  const typingFor = nextIdx >= 0 && bubbles[nextIdx].from === "them" && t > (bubbles[nextIdx].appearSec - 1.1);
  return (
    <AbsoluteFill style={{background: "radial-gradient(ellipse at 30% 20%, #1a1a1e 0%, #0a0a0b 70%)", alignItems: "center", justifyContent: "center"}}>
      <div style={{width: 460, height: 880, background: "#f6f6f8", borderRadius: 54, border: "10px solid #2a2a2e", boxShadow: "0 40px 120px rgba(0,0,0,0.8)", overflow: "hidden", transform: "rotate(-1.5deg)"}}>
        <div style={{background: "#fbfbfd", padding: "18px 22px 10px", borderBottom: "1px solid #e2e2e6", textAlign: "center"}}>
          <div style={{fontFamily: F.label, fontSize: 15, color: "#8a8a90", display: "flex", justifyContent: "space-between"}}>
            <span>{clock}</span><span>●●●</span>
          </div>
          <div style={{width: 44, height: 44, borderRadius: 22, background: "#c9c9cf", margin: "8px auto 4px", display: "flex", alignItems: "center", justifyContent: "center", color: "#fff", fontFamily: F.label, fontSize: 22}}>?</div>
          <div style={{fontFamily: F.label, fontSize: 17, color: "#1b1b1e", fontWeight: 600}}>{header}</div>
        </div>
        <div style={{padding: "22px 18px", display: "flex", flexDirection: "column", gap: 12}}>
          {bubbles.map((b, i) =>
            t >= b.appearSec ? (
              <Build key={i} startSec={b.appearSec} durSec={0.35} from="up">
                <div style={{
                  maxWidth: "78%", padding: "12px 16px", borderRadius: 20, fontFamily: F.label, fontSize: 20, lineHeight: 1.35,
                  alignSelf: b.from === "them" ? "flex-start" : "flex-end",
                  marginLeft: b.from === "them" ? 0 : "auto",
                  background: b.from === "them" ? "#e9e9ec" : "#31c04f",
                  color: b.from === "them" ? "#151517" : "#fff",
                }}>
                  {b.text}
                </div>
              </Build>
            ) : null
          )}
          {typingFor ? (
            <div style={{alignSelf: "flex-start", background: "#e9e9ec", borderRadius: 20, padding: "14px 18px", display: "flex", gap: 5}}>
              {[0, 1, 2].map((d) => (
                <div key={d} style={{width: 9, height: 9, borderRadius: 5, background: "#9a9aa2", opacity: 0.4 + 0.6 * Math.abs(Math.sin((frame / fps) * 4 + d))}} />
              ))}
            </div>
          ) : null}
        </div>
      </div>
      {illustrative ? (
        <div style={{position: "absolute", bottom: 70, background: C.redDeep, color: "#fff", fontFamily: F.label, fontWeight: 800, fontSize: 22, padding: "8px 18px", letterSpacing: "0.06em"}}>
          RECREATION — message text from case records
        </div>
      ) : null}
    </AbsoluteFill>
  );
};
