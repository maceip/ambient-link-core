# core-android

Shared, vendor-neutral Android library for Ambient Link — the single source of
truth that ends the `RelayClient`/`Session` duplication across the glasses app,
the Wear watch, and (later) the Meta Android relay.

This is migration step 3 from [`../ROUTING.md`](../ROUTING.md).

## Contents (`com.ambientlink.core`)

| Type | Purpose |
|---|---|
| `Session` | relay session model (agent/cwd/state/preview, `isLive`, `shortCwd`, `label`) |
| `RelayClient` | polls `GET {base}/ambient-link/status`, exposes `sessions: StateFlow` |
| `GlassLink` | vendor-neutral capture contract (from the Cosmo teardown) |
| `EphemeralBuffer<T>` | TTL ring buffer (Cosmo `InMemoryEphemeralBuffer`) |
| `Throttle` | per-key leading-edge frame gate (Cosmo `FRAME_PROCESS_INTERVAL_MS`) |
| `WearPaths` + status/trigger enums | `/ambientlink/...` data-layer protocol |
| `SttEngine` | vendor-neutral streaming speech-to-text contract (begin → partial* → commit) |

`kotlinx-coroutines` is exposed as `api`, so consumers get `StateFlow` transitively.

## Consuming it

**Composite build (no publish step — recommended for local dev).** In the vendor
repo's `settings.gradle.kts`:

```kotlin
includeBuild("../ambient-link-core/core-android")
```

and in the module `build.gradle.kts`:

```kotlin
implementation("com.ambientlink:core-android:0.1.0")
```

Gradle substitutes the dependency with the local build automatically (matched on
`com.ambientlink:core-android`). Requires `ambient-link-core` checked out as a
sibling of the vendor repo.

**Maven (CI / release).** Publish once, then depend via `mavenLocal()` /
your repo:

```bash
./gradlew publishToMavenLocal
```

## Status

`ambient-link-google` (`:app` + `:wear`) consumes this and no longer carries its
own `RelayClient`/`Session`. `ambient-link-meta/relay-android` still has its own
WS-oriented relay stack; folding its `GlassLink`/`Session` onto this library is the
next step.
