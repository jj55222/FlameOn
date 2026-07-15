import React from "react";
import {AbsoluteFill, Sequence} from "remotion";
import {Caption911, FootageBadge} from "./CaptionVoices";
import {DocQuote} from "./DocQuote";
import {RoleLowerThird} from "./RoleLowerThird";
import {SatelliteLocator} from "./SatelliteLocator";
import {C} from "./theme";

/** Sign-off demo reel: DocQuote -> SatelliteLocator (real NAIP if fetched) ->
 * lower third + caption voices on a dark stand-in frame. ~30s total @30fps. */
export const MotifDemo: React.FC<{hasAerial: boolean}> = ({hasAerial}) => (
  <AbsoluteFill style={{backgroundColor: C.bg}}>
    <Sequence from={0} durationInFrames={10 * 30}>
      <DocQuote
        kickerText="WHAT THE DEPUTIES FOUND"
        headline="DEPUTY MARVIN MORALES"
        docLabel="INTERNAL AFFAIRS · INVESTIGATION REPORT"
        quote="Deputy Morales was discovered by his partners with his gun belt completely removed and positioned on top of the baby changing station inside the stall, as well as his uniform pants unbuttoned and unzipped."
        redPhrases={["gun belt completely removed", "unbuttoned and unzipped"]}
        analysis="Investigators read the stripped gear as a sign of intentional use — not an accident."
        source="Internal Affairs Report · 2013PSB-0530 · p.14"
      />
    </Sequence>
    <Sequence from={10 * 30} durationInFrames={13 * 30}>
      {hasAerial ? (
        <SatelliteLocator
          levels={[
            {file: "aerial_4s/4s_wide.png", half_width_m: 1400},
            {file: "aerial_4s/4s_mid.png", half_width_m: 420},
            {file: "aerial_4s/4s_tight.png", half_width_m: 150},
          ]}
          dateTime="DEC 07 2023 · 23:23"
          place="4S COMMONS DRIVE — RALPHS"
          coords="33.0219 N · 117.1015 W"
          person={{name: "CURTIS HARRIS"}}
          illustrated={false}
          dots={[
            {mode: "path", color: "#d81f26", label: "HARRIS", trail: true, durSec: 4.2,
             points: [[38, 24], [43, 33], [47, 41], [46, 52]], startSec: 0.4},
            {mode: "free", color: "#35c8e8", label: "OFFICER 2", wanderPct: 1.0,
             points: [[62, 58]], startSec: 1.2},
          ]}
        />
      ) : (
        <AbsoluteFill style={{alignItems: "center", justifyContent: "center", color: C.dim, fontSize: 40}}>
          run tools/fetch_aerial.py first
        </AbsoluteFill>
      )}
    </Sequence>
    <Sequence from={23 * 30} durationInFrames={11 * 30}>
      <AbsoluteFill style={{background: "linear-gradient(180deg,#141416,#0a0a0b)"}}>
        <FootageBadge />
        <RoleLowerThird role="RESPONDING DEPUTY" quote="Leave that door open so it airs out." startSec={0.4} />
        <Caption911 text="Austin 911, what's your emergency?" startSec={4.5} />
      </AbsoluteFill>
    </Sequence>
  </AbsoluteFill>
);
