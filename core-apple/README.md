# core-apple (`AmbientLinkCore`)

Shared, vendor-neutral SwiftPM package for Apple platforms — the single source of
truth that ends the `Session`/`RelayClient`/`GlassLink` duplication across
`ambient-link-apple` and `ambient-link-meta/relay-ios`.

This is migration step 4 from [`../ROUTING.md`](../ROUTING.md) — the Swift peer of
[`../core-android`](../core-android).

## Contents (`import AmbientLinkCore`)

| Type | Purpose |
|---|---|
| `Session` | relay session model (`isLive`, `shortCwd`, `label`) + `{sessions:[…]}` decode |
| `RelayClient` | `GET /ambient-link/status`, `POST /ambient-link/ingest` |
| `SessionStore` | `@MainActor @Observable` polling store for SwiftUI |
| `GlassLink` / `GlassFrame` | vendor-neutral capture contract (from the Cosmo teardown) |
| `EphemeralBuffer<T>` | TTL buffer (Cosmo `InMemoryEphemeralBuffer`) |
| `Throttle` | per-key leading-edge frame gate (Cosmo `FRAME_PROCESS_INTERVAL_MS`) |

Apple-vendor-specific code stays out of here: Foundation Models ("Siri AI"),
App Intents, and the visionOS frontend live in `ambient-link-apple`'s
`AmbientLinkKit`, which depends on this package.

## Consuming it

SwiftPM local path dependency (no publish step; requires `ambient-link-core` as a
sibling checkout):

```swift
.package(path: "../ambient-link-core/core-apple")
// target deps:
.product(name: "AmbientLinkCore", package: "core-apple")
```

`AmbientLinkKit` re-exports it (`@_exported import AmbientLinkCore`) so the visionOS
app keeps seeing `Session`/`RelayClient`/`SessionStore` through `import AmbientLinkKit`.

## Build

```bash
swift build
swift test
```

## Status

`ambient-link-apple` consumes this and no longer carries its own
`Session`/`RelayClient`/`GlassLink`. `ambient-link-meta/relay-ios` is loose Swift
sources (no in-repo Xcode project); its `GlassLink.swift` copy should be replaced
with this package once that target gets a build file.
