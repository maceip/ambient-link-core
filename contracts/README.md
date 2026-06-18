# contracts

Canonical, vendor-neutral contracts implemented by every Ambient Link surface.
**Source of truth** — copy the matching language file into each vendor repo and
implement it; do not fork the shape.

| File | Implemented by |
|---|---|
| `GlassLink.kt` / `EphemeralBuffer` | `ambient-link-google` (ProjectedXR), `ambient-link-meta/relay-android` (DAT) |
| `GlassLink.swift` | `ambient-link-apple`, `ambient-link-meta/relay-ios` |
| `glass-link.ts` | `ambient-link-snapchat` (Lens Studio) |
| `wear-data-layer.md` | `ambient-link-google/wear` (phone-tethered path) |

Rationale, perf patterns, and the repo reorg plan: see [`../ROUTING.md`](../ROUTING.md).

These are intentionally header-only (interfaces/protocols + doc). They carry no
vendor imports so they stay copy-pasteable. When `core-android` / `core-apple`
shared libraries land, these graduate into real published modules.
