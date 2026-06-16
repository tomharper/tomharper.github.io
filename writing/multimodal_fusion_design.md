---
layout: single
title: "Multimodal Fusion & Co-Sequencing"
permalink: /writing/multimodal-fusion/
toc: true
---

# Multimodal Conversational Agent — Fusion & Co-Sequencing Design

A design doc for a messaging service that handles **text, voice, and video at
once** and can **escalate** between them, where the streams carry
**complementary** information (words, tone, what's being shown) that must be
**fused on a shared event-time timeline** before reaching the LLM.

Centerpiece: **multimodal fusion + co-sequencing.** Escalation tiers and batch
transcription are supporting sections.

---

## 0. The thesis (say this first)

> **Everything becomes tokens at the end — but the modalities are not redundant
> transcripts to dedup. They are complementary signals to *fuse*. Each
> specialist model compresses its channel into structured annotations on a
> shared event-time timeline; a sequencer aligns and bundles co-temporal
> annotations into enriched tokens; the LLM consumes one fused stream. The two
> hard parts are (a) aligning streams that have different latencies and rates on
> *event-time* and (b) choosing what survives each channel's compression —
> because the whole point is to keep the signal that lives in the audio/video
> but not the words.**

Two reframes that drive everything below:
1. **Co-sequencing is a stream-join on event-time, not processing-time.** (Flink/
   Dataflow watermark model.)
2. **Escalation is a resource-tier upgrade of one persistent session.** Text =
   cheap async tier; voice/video = expensive real-time tier. Same brain,
   different transport + latency budget + cost.

---

## 1. Why naive ordering is wrong

The modalities don't carry the same thing:

| Channel | Carries | Example signal |
|---|---|---|
| text / STT | **propositional** — the literal words | "this is fine" |
| audio prosody | **affective / pragmatic** — tone, stress, hesitation | clipped, ↑pitch, anger 0.8 |
| video | **referential / deictic** — what's shown / pointed at, expression | points at a fee, frustrated face |

`"this is fine"` + angry tone + frustrated face is a **complaint**. The
transcript alone reads as **approval**. For a bank, that's the difference
between catching a churning customer and missing them. The non-verbal signal is
*exactly* the thing absent from the words — so you cannot flatten to text and
order by arrival.

---

## 2. The core problem: event-time vs processing-time

Streams arrive on separate async paths with **different, structural latencies**:

```
 user types    "actually no—"     ───────────────●──────────▶  ~instant
 user speaks   "send it to..."  ──────●·······▶ +80ms STT ────▶ arrives late
 user gestures [points at X]    ──●·······▶ +200ms vision ─────▶ arrives later
                                  │     │       │
                              EVENT    EVENT   EVENT  time it actually happened
                                  └─────┴───────┘
                  order by EVENT time, NOT by which pipeline finished first
```

Order by **arrival** and the instant text "actually no—" lands *before* the
slow-transcribed speech it was correcting → the LLM sees the conversation
**backwards**. Order by **event-time** and it's correct.

### The concrete timing model (the crux)
Processing latency is **per-modality and roughly constant**:

- **audio integration ≈ 80 ms**
- **visual integration ≈ 200 ms**

So a sound and a sight that were **simultaneous at the edge** arrive at the
sequencer **~120 ms apart**. This is a *known structural offset*, not random
jitter — which makes it correctable by construction (same idea as A/V lip-sync
in a video player).

**Two consequences that define the design:**

1. **Latency calibration → recover event-time.** Stamp at capture, then
   subtract the channel's known latency:
   ```
   event_time = arrival_time − stream_latency      # audio −80ms, visual −200ms
   ```
   Now a co-temporal sound + gesture land on the same event-time axis and fuse
   correctly.

2. **Reorder window ≥ max inter-stream skew.** You must hold the timeline open
   at least ~120 ms (visual − audio, + jitter margin) before committing, or the
   slower visual event arrives *after* you flushed the audio it belonged with.
   → **The commit delay is set by your SLOWEST modality (visual, 200ms), not the
   fastest.** Your fusion window can never be tighter than your slowest channel.

This is the latency-vs-correctness knob, made physical.

---

## 3. Architecture

```
  EDGE (stamp event-time at capture)
   mic ──┐        camera ──┐        keyboard ──┐
         ▼                 ▼                    ▼
  ┌─────────────┐  ┌─────────────┐     ┌─────────────┐
  │ STT + prosody│  │ vision model │     │  text (0 lag)│   ← SPECIALISTS
  │  ~80ms       │  │  ~200ms      │     │              │     each compresses
  │ words+affect │  │ refs+affect  │     │   tokens     │     its channel to
  └──────┬───────┘  └──────┬───────┘     └──────┬──────┘     structured tags
         │ annotations     │ annotations        │
         └─────────────────┼────────────────────┘
                           ▼
                ┌────────────────────────┐
                │   SEQUENCER             │  • calibrate: arrival − latency
                │   (event-time join)     │  • buffer in a reorder window
                │                         │  • WATERMARK: commit prefix when
                │                         │    "seen everything ≤ T"
                │                         │  • bundle co-temporal annotations
                └───────────┬─────────────┘    into ONE enriched token
                            ▼
                ┌────────────────────────┐
                │   FUSION                │  late fusion: merge the tags into
                │   (annotations→context) │  LLM-readable context per event
                └───────────┬─────────────┘
                            ▼
                ┌────────────────────────┐      ┌──────────────────────┐
                │   SHARED LLM (batched)  │◀────▶│  SESSION STORE        │
                │   KV-cache = session    │      │  id·identity·history· │
                │   working memory        │      │  state (transport-    │
                └───────────┬─────────────┘      │  agnostic, survives   │
                            ▼                     │  modality switch)     │
                  response → TTS / text           └──────────────────────┘
```

---

## 4. The Sequencer (the heart)

A k-way merge keyed on **event-time**, with a watermark commit. Structurally the
same "don't finalize while work is in flight" as the web-crawler's `Queue.join()`
— here the in-flight thing is *a stream that hasn't reported up to time T yet*.

```
on event e from stream s:
    e.event_time = e.arrival_time - LATENCY[s]      # calibrate
    buffer.push(e)                                   # min-heap on event_time
    watermark[s] = e.event_time                      # this stream is current to here
    W = min(watermark over all active streams)       # global safe time
    while buffer.peek().event_time <= W:             # everything ≤ W has arrived
        emit(buffer.pop())                           # commit in event-time order
```

- **Watermark `W = min over streams`** — you can only safely commit up to the
  point your *slowest-reporting* stream has reached. One quiet stream holds the
  line (same straggler dynamic as the training all-reduce barrier).
- **Idle-stream timeout.** If a stream goes silent (user stops gesturing), its
  watermark would freeze the timeline forever. Advance it with a heartbeat /
  max-wait so a silent modality can't stall the conversation. (Mirrors the
  batcher's size-OR-timer: commit on watermark OR deadline.)
- **Co-temporal bundling.** Events within a small ε window at the same event-time
  are fused into one enriched token rather than emitted separately (the prosody
  spike *belongs to* that word).

---

## 5. Fusion: early vs late

```
   EARLY fusion                         LATE fusion  ✅ (build this)
   raw features → one big              each stream → its specialist →
   multimodal model                    structured tags → fuse the tags
   richest, $$$, tightly coupled,      cheap, modular, swappable models,
   one model must do everything        degrades gracefully if one drops
```

**Late fusion** is the practical choice. Each specialist emits typed annotations;
the token becomes a multi-channel record:

```
[t=4.10  words="this is fine"]
[t=4.10  prosody={anger:0.8, certainty:low}]
[t=4.10  visual={ref:"fee_line_item", affect:"frustrated"}]
   → fused LLM context:
     User said "this is fine" but tone=angry and pointing at the fee
     → likely sarcastic; treat as a complaint about the fee.
```

### The two hard parts late fusion exposes
1. **Alignment across rates.** Prosody is continuous (per-frame), words are
   discrete (per-utterance), gestures are sparse (per-event). Binding "this anger
   spike → *that* word" is a **windowed join on event-time** within a tolerance —
   the sequencer joining *features to tokens*, not just ordering tokens.
2. **Compression: what survives.** You can't send raw audio/video to the LLM —
   it's tokens at the end. Each specialist compresses its channel to a few tags,
   and the design question is *which signal survives*. The emotion/referent is
   precisely what's in the media but not the transcript — lose it in compression
   and you've discarded the reason the channel exists. **Design the tag schema
   around the signal that's unique to each channel.**

---

## 6. Escalation: tiers of one persistent session

The session store is the **source of truth** and is **transport-agnostic** — so
moving text→voice→video is binding a new transport to existing state, not a
rewrite. This is the load-bearing decision (make it in step 2 below).

| Tier | Transport | Latency budget | Cost | Concurrency/GPU |
|---|---|---|---|---|
| **Text** | WS/HTTP | seconds | cheap | many |
| **Voice** | WebRTC: VAD→STT→LLM→TTS | sub-second (mouth-to-mouth) | GPU slot | few |
| **Video** | + vision specialist | sub-second + 200ms vision | GPU slot ++ | fewer |

- **Escalation triggers:** explicit ask · detected frustration (now *measurable*
  from the prosody/visual tags!) · task complexity · identity verification ·
  human handoff.
- **Handoff:** provision the tier's GPU slot, attach the existing session
  (history + intent + fused affect state) — the voice tier starts already knowing
  the user is frustrated about the fee. Downgrade path symmetric.
- **Barge-in re-sequences, not just cancels.** When the human interrupts the bot,
  you (1) cancel TTS instantly (`10_audio_pipeline_interruptible` — single Event,
  single-chunk latency) AND (2) treat the interruption as a new high-priority
  event that may **reorder/discard buffered tokens** in the sequencer.

---

## 7. Build order (the "layer a constraint" rhythm)

Each step runs before the next exists — present it this way; let the interviewer
"add a requirement" and each requirement is the next step.

1. **Text messaging** — session store + stateless handlers + LLM. Baseline.
2. **Persistent, transport-agnostic session** — `{id, identity, history,
   state}`; workers stateless. ← *the decision that makes everything else cheap.*
3. **Voice channel** — WebRTC + VAD→STT→LLM→TTS, reusing the same store + LLM.
4. **Sequencer** — event-time calibration + watermark commit; needed the moment
   two streams coexist.
5. **Fusion** — specialists emit tags; bundle co-temporal annotations; design the
   tag schema around channel-unique signal.
6. **Video channel** — add the 200ms vision specialist; the sequencer's window
   widens to its latency; nothing else changes (that's the payoff of step 4).
7. **Escalation controller** — triggers + handoff + downgrade.
8. **Real-time hardening** — turn-taking/endpointing, barge-in re-sequencing, AEC3.
9. **Scale & economics** — scheduler (text tier vs GPU voice/video slots), batch
   the shared LLM, persist KV-cache across escalation (~7× headroom from KV-cache
   compression — see voice_agents_notes.md), cost per session per tier.

---

## 8. Connections to the rest of the prep set

| This design | Reuses pattern from |
|---|---|
| Sequencer watermark commit | `09_web_crawler` `Queue.join()` (don't finalize in-flight) |
| Watermark = min over streams; idle straggler | `15_training_pipeline` all-reduce barrier |
| Commit on watermark OR deadline | `14_inference_server` size-OR-timer trigger |
| Barge-in cancel | `10_audio_pipeline_interruptible` (single Event) |
| Shared LLM, KV-cache = session memory | `14_inference_server` (token = unit of work) |
| Tiers = work units → capacity units | README **resource scheduler** thesis |
| Event-time merge (heap) | `algorithms_cheatsheet` heap / k-way merge |

---

## 9. Things to say out loud (interview ammo)

- "This is a **stream-join on event-time** — the modalities are sources; the hard
  part is the reorder window and the watermark that decides when the timeline is
  safe to commit."
- "Audio is ~80ms, visual ~200ms, so simultaneous edge events arrive ~120ms
  apart. I **calibrate** by subtracting per-stream latency and size the **reorder
  window to the slowest modality** — correctness is bounded by your slowest
  channel."
- "I use **late fusion** — specialists emit structured tags — so models are
  swappable and it degrades gracefully. The schema is designed around the signal
  unique to each channel: tone from audio, referents from video."
- "Escalation is a **tier upgrade of one persistent session**, not a new
  conversation — which is why I make the session store transport-agnostic on day
  one."
- "Barge-in doesn't just cancel TTS — it **re-sequences**, because the
  interruption is a new high-priority event in the timeline."

---

## Open questions for the next pass
- Mouth-to-mouth latency target (banks usually want <800ms) → budget breakdown
  across VAD/STT/LLM/TTS + network + the 200ms vision tax.
- Self-host vs API per hop (see voice_agents_notes.md cost tables).
- Tag schema: exact fields per modality; how much affect detail the LLM can
  actually use.
- Failure modes: one specialist down (degrade to remaining channels), watermark
  starvation, clock skew across edge devices.
