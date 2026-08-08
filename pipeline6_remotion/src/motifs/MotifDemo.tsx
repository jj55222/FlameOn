import React from "react";
import {AbsoluteFill, Sequence} from "remotion";
import {Caption911, CaptionSFX, FootageBadge} from "./CaptionVoices";
import {DiagramAnnotate} from "./DiagramAnnotate";
import {RouteTracePiP} from "./EvidencePiP";
import {MessageThread} from "./MessageThread";
import {DocQuote} from "./DocQuote";
import {EvidenceBoard} from "./EvidenceBoard";
import {ComingUpGrid, ProgressInterstitial, RecordsArtifact, TitleCard} from "./Interstitials";
import {PaperBoard} from "./PaperBoard";
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
        <CaptionSFX text="gunshots" startSec={8.6} bottom={190} />
      </AbsoluteFill>
    </Sequence>

    <Sequence from={76 * 30} durationInFrames={9 * 30}>
      <MessageThread
        header="Unknown Number"
        bubbles={[
          {text: "Hi Kristil, it's Anthony.", from: "them", appearSec: 1.4},
          {text: "Hope it's OK i looked you up.", from: "them", appearSec: 3.2},
          {text: "I go to Boulder every few weeks and thought we should hook up. U game?", from: "them", appearSec: 5.4},
        ]}
      />
    </Sequence>

    <Sequence from={85 * 30} durationInFrames={9 * 30}>
      <RouteTracePiP
        aerial="aerial_4s/4s_mid.png"
        route={[[18, 78], [30, 64], [42, 55], [50, 44], [49, 33]]}
        durSec={5.5}
        pipImage="board_demo/card_pallets.jpg"
        pipLabel="OFFICER 2 — BWC"
        header="THE APPROACH"
      />
    </Sequence>

    <Sequence from={94 * 30} durationInFrames={9 * 30}>
      <DiagramAnnotate
        page="board_demo/doc_page1.png"
        markers={[
          {at: [46, 38], appearSec: 1.4, label: "entry"},
          {at: [58, 52], appearSec: 3.0, label: "exchange"},
          {at: [52, 70], appearSec: 4.6, label: "aid rendered", color: "#d81f26"},
        ]}
        legend="● markers keyed to CAD + BWC positions"
        source="Case report p.1 · SDPD 4S Commons"
        glowAt={[52, 52]}
      />
    </Sequence>

    {/* EvidenceBoard — narration-synced: cards/links appear as the narrator names them.
        Sample narration: "Harris was last seen in aisle four... he came out the front —
        straight into the officers waiting behind the pallets... and Officer 4 held the line." */}
    <Sequence from={34 * 30} durationInFrames={13 * 30}>
      <EvidenceBoard
        title="THE CHAIN"
        cards={[
          {id: "harris", label: "CURTIS HARRIS", sublabel: "subject · store CCTV", image: "board_demo/card_aisle.jpg", at: [22, 42], appearSec: 1.2},
          {id: "pallets", label: "THE PALLET LINE", sublabel: "Officer 2 BWC · 23:54", image: "board_demo/card_pallets.jpg", at: [52, 30], appearSec: 3.4},
          {id: "cover", label: "OFFICER 4 — COVER", sublabel: "BWC · 23:54:19", image: "board_demo/card_cover.jpg", at: [78, 52], appearSec: 5.6},
          {id: "weapon", label: "THE WEAPON", sublabel: "not shown in released bundle", at: [40, 72], appearSec: 7.8},
        ]}
        links={[
          {from: "harris", to: "pallets", appearSec: 4.2, label: "out the front"},
          {from: "pallets", to: "cover", appearSec: 6.4},
          {from: "harris", to: "weapon", appearSec: 8.6, label: "per IA report"},
        ]}
        verdict={{text: "SHOOTING RULED WITHIN POLICY — DA REVIEW", appearSec: 10.6}}
        source="Illustrative demo · frames from released 4S bundle"
      />
    </Sequence>

    <Sequence from={47 * 30} durationInFrames={8 * 30}>
      <PaperBoard photo="board_demo/card_aisle.jpg" name="Curtis Harris" role="the subject" caption="Store cameras caught him minutes before officers arrived." />
    </Sequence>

    <Sequence from={55 * 30} durationInFrames={6 * 30}>
      <RecordsArtifact caseNo="SDPD 23-2401xx" rows={[["CAD Event", "23-340576"], ["Crime Report", "23-2529xx"], ["911 Calls", "51 files"], ["IA Review", "DA-2024-011"]]} />
    </Sequence>

    <Sequence from={61 * 30} durationInFrames={4 * 30}>
      <ProgressInterstitial pct={38} brand="FLAMEON" />
    </Sequence>

    <Sequence from={65 * 30} durationInFrames={5 * 30}>
      <TitleCard wordmark="FLAME ON" credit="THIS IS A FLAMEON PRODUCTION" />
    </Sequence>

    <Sequence from={70 * 30} durationInFrames={6 * 30}>
      <ComingUpGrid tiles={["board_demo/card_pallets.jpg", "aerial_4s/4s_tight.png", "board_demo/card_aisle.jpg", "board_demo/card_cover.jpg", "aerial_4s/4s_mid.png", "aerial_4s/4s_wide.png"]} />
    </Sequence>
  </AbsoluteFill>
);
