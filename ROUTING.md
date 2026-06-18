# Routing, Companion Links & Repo Reorg

Canonical architecture for how Ambient Link moves data between the host relay and
every wearable/companion surface — and how the repos are organized to keep that
fast and vendor-clean.

This plan is grounded in the recovered **Cosmo** artifacts (see
`ambient-link-google/glasses_link.md`). Cosmo is Google's shipping ambient-AI app;
its glasses/watch/buds plumbing is the strongest real-world evidence we have for
the right shape. The takeaways below are lifted directly from that teardown.

---

## 1. The load-bearing lesson: transport is vendor-hidden; the *interface* is ours

Cosmo exposes **no** raw BLE/RFCOMM/TCP/WebSocket for glasses. The glasses link is:

```
Cosmo app process -> Android projected activity/service APIs -> vendor ProjectedGlassCaptureService -> (radio hidden in vendor layer)
```

The app only ever touches a **service boundary with a stable public shape**. The
actual radio is below the vendor service and not in app code.

**Consequence for us — this *is* the core/vendor split:**

- `ambient-link-core` owns the **contracts** (the stable public shape) + the relay
  + STT. It never imports a vendor SDK.
- `ambient-link-{meta,google,apple,snapchat}` each own **one implementation** of
  those contracts against their device's real transport (DAT, ProjectedXR,
  RealityKit/Spectacles), plus their display layer.

We do not try to reimplement vendor radios. We standardize the boundary above them.

---

## 2. The companion-link contract (extracted public shape)

From Cosmo's `CosmoGlassManager` + the report's "Practical Reimplementation Targets",
the minimal vendor-agnostic surface is:

```
GlassLink
  connected : StateFlow<Bool>          // device present (e.g. ProjectedContext.isProjectedDeviceConnected)
  bound     : StateFlow<Bool>          // capture service bound / session live
  bind() / unbind()
  setupImageCapture(onFrame)           // register frame sink
  startImageCapture() / stopImageCapture()
  startAudioCapture(onBytes) / stopAudioCapture()
  clear()                              // drop buffers, reset gates
```

- **Reactive gates** (`connected`, `bound`) drive everything; nothing polls a bool.
- **Callbacks, not return values**, for media (frames/audio bytes) so producers
  push at their own cadence and consumers apply backpressure.
- Same shape works for glasses, watch mic, and buds — they differ only in transport.

Canonical contracts live in [`contracts/`](contracts/) in three languages
(Kotlin / Swift / TypeScript) so every vendor repo implements the *identical* surface.

---

## 3. Performance & backpressure patterns (the reason this report matters)

Cosmo's whole pipeline is built to not melt the phone. Copy these everywhere a
surface ingests media:

| Pattern | Cosmo evidence | Where we apply it |
|---|---|---|
| **Frame throttling** | `FRAME_PROCESS_INTERVAL_MS = 10s`, `GLASS_CAMERA_TARGET_FPS = 0.1` | any glasses/camera frame sink — default 1 frame / 10s, configurable |
| **TTL ephemeral buffers** | `InMemoryEphemeralBuffer<Bitmap>`, `getEphemeralBufferDurationMin` | core `EphemeralBuffer<T>` contract; vendor impls store frames/audio here, auto-expire |
| **Channel-scoped streaming** | Wear `/cosmowear/mic_stream` over `ChannelClient`, stopped on `/mic_stream_stop` | watch audio + any high-rate stream: dedicated channel, explicit stop, no polling |
| **Foreground services for capture** | `ProjectedGlassCaptureService` (mic+camera), `WearableAudioStreamService` (dataSync) | long-running capture must be a typed foreground service, never a bare coroutine |
| **StateFlow gates over polling** | bound/connected/should-run flows | all link state is `StateFlow`; UI and routing react, never spin |
| **Single-bind guard** | `bindGlassCaptureService()` no-ops if bound or binding-in-flight | every `bind()` impl must be idempotent |
| **Mutex + job + socket-close on raw sockets** | Buds RFCOMM `connectionMutex`, `connectionJob`, close handling | any raw-socket fallback impl |
| **Settings gate per path** | `isConnectToGlassEnabled`, `isGlassesCameraEnabled`, … | each link is independently toggleable; routing checks the gate before binding |

**Routing rule of thumb:** prefer the *cheapest reachable* transport, gate it, and
stream over a dedicated channel with explicit teardown. Never hold a connection a
gate says should be off.

**Realized in Go transit (tested):** `host/internal/backpressure` implements the two
primitives — `EphemeralBuffer[T]` (TTL + max) and `Throttle` (per-key leading-edge
gate) — with unit tests. The throttle is wired into the **dictation partial fan-out**
(`host/internal/dictate`): the high-rate `dictate_partial` firehose is gated to
~6.7/s per thread while `begin`/`commit`/`abort` always pass and reset the gate
(commit carries the full transcript, so no data is lost). Verified live: a 30-partial
burst over the WS collapsed to **1** fanned-out frame; the 9-test protocol
conformance suite stays green against the rebuilt host.

---

## 4. Multi-source audio fan-in to one STT

Cosmo folds **glass audio, watch audio, and buds audio** into one SODA pipeline via
per-source captors (`SODAAmbientAudioCaptor`, `SODAWatchAudioCaptor`,
`SODABudsAudioCaptor`) with Hilt qualifiers keeping the streams separate.

For us:

- Core owns **one STT entry point** (`dictate/` today; SODA promoted to core per the
  reorg decision). Every audio source — glasses mic, watch mic, phone mic — feeds the
  same `onBytes` sink and the same transcription pipeline.
- Sources stay separate only by **qualifier/label**, not by forking the pipeline.
- This is why STT belongs in core, not in any one vendor app.

---

## 5. Wear OS data-layer protocol (replaces naive Wi-Fi polling)

Cosmo's watch link is a typed protocol over the Play Services Wearable Data Layer
(`NodeClient`/`MessageClient`/`ChannelClient`) on stable `/cosmowear/...` paths.
Our watch currently polls the relay over Wi-Fi — fine for standalone, but the
**phone-tethered** path should mirror Cosmo and proxy through the phone for battery
and latency.

We define an analogous **`/ambientlink/...`** path set — see
[`contracts/wear-data-layer.md`](contracts/wear-data-layer.md). Watch reads sessions
and sends quick replies/triggers over messages; high-rate audio uses a channel with
an explicit stop, exactly like `/cosmowear/mic_stream` + `/mic_stream_stop`.

---

## 6. Target repo shape

```
ambient-link-core                      VENDOR-NEUTRAL ONLY
  protocol/         relay wire protocol + conformance test
  contracts/        GlassLink + EphemeralBuffer + wear-data-layer  (Kotlin/Swift/TS)
  host/             Go relay daemon (producers, delivery, sink, mux, journal, mdns, pair)
    dictate/        STT entry point  (+ SODA engine, promoted from meta — decided)
  tools/            relay-bridge, ws-check
  core-android/     shared Kotlin lib: RelayClient + Session + GlassLink + EphemeralBuffer + Throttle + WearPaths
  core-apple/       shared Swift pkg (AmbientLinkCore): same surface for Apple platforms

ambient-link-meta                      META-SPECIFIC
  web/              Ray-Ban Display web app
  relay-android/    DAT display (hud/, wearables/) + GlassLink(DAT) impl
  relay-ios/        DAT display + GlassLink impl

ambient-link-google                    GOOGLE-SPECIFIC
  app/    Android XR projected glasses: ProjectedGlassLink (mirrors CosmoGlassManager)
  wear/   Wear OS watch: /ambientlink data-layer (mirrors /cosmowear)

ambient-link-apple                     APPLE-SPECIFIC
  visionOS frontend + Foundation Models (Siri) + App Intents
  GlassLink(Swift) over RealityKit/audio

ambient-link-snapchat                  SNAP-SPECIFIC
  Lens Studio (SIK/SUIK) + Fetch API + GlassLink(TS)
```

### Migration order (low-risk first)

1. **Done:** move relay tooling + protocol test into core.
2. **Done:** add `contracts/` to core; copy the contract into every vendor repo and
   make existing clients implement it.
3. **Done:** extract `core-android` (`Session`, `RelayClient`, `GlassLink`,
   `EphemeralBuffer`, `Throttle`, `WearPaths`). `ambient-link-google` (`:app` +
   `:wear`) consumes it via composite build (no more `RelayClient`/`Session` dup);
   `ambient-link-meta/relay-android` consumes it from mavenLocal and dropped its
   `GlassLink` copy. meta's `RelayClient` stays vendor-specific (WS/OkHttp, not the
   polling client).
4. **Done:** graduate the shared Swift into `core-apple` (`AmbientLinkCore`:
   `Session`, `RelayClient`, `SessionStore`, `GlassLink`, `EphemeralBuffer`,
   `Throttle`). `ambient-link-apple`'s `AmbientLinkKit` depends on it via a SwiftPM
   path dependency and re-exports it (Foundation Models / App Intents / visionOS stay
   Apple-specific). `relay-ios` still has loose Swift sources with no in-repo build
   file — its `GlassLink.swift` folds in once that target gets a Package/xcodeproj.
5. Promote SODA STT into `core-android` (decided); vendor apps consume it.
   (meta `relay-android` already carries a recovered SODA engine — that's the source.)

Each step is independently shippable.

> **core-android consumption — two paths (AGP version decides):**
> - *Composite build (no publish step):* when the consumer's AGP matches
>   core-android's (8.7) — e.g. `ambient-link-google`. Add
>   `includeBuild(".../core-android")` + `implementation("com.ambientlink:core-android:0.1.0")`;
>   Gradle substitutes the coordinate with the local build.
> - *Published AAR (mavenLocal / GitHub Packages):* when AGP skews — e.g.
>   `ambient-link-meta` on AGP 8.6. AGP forbids two AGP versions in one composite
>   invocation, so consume the binary. One-time `./gradlew publishToMavenLocal` for
>   local dev; CI resolves from a real registry. (jvmTarget skew is irrelevant on
>   Android — everything dexes to one target.)

---

## 7. What we explicitly do NOT copy

From the report's "Not proven" section: there is no app-owned glasses radio to clone.
We do **not** invent a BLE/socket glasses transport. We implement `GlassLink` against
each vendor's sanctioned API and provide a fallback impl only for hardware that
exposes camera/audio directly. Keep buds/RFCOMM-style sources as *audio inputs*, never
as a glasses transport.
