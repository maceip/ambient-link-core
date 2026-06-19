# contracts

Vendor-neutral contracts for Ambient Link. There is **one home per language**, and
for Kotlin/Swift that home is now the shared library — not this directory.

| Contract | Single source of truth | Implemented by |
|---|---|---|
| `GlassLink`, `EphemeralBuffer`, `Throttle`, `Session`, `RelayClient`, `WearPaths`, `SttEngine` | **`../core-android`** (`com.ambientlink.core`) | `ambient-link-google` (ProjectedXR); `ambient-link-meta/relay-android` (DAT + SODA `SttEngine`) |
| same surface, Swift | **`../core-apple`** (`AmbientLinkCore`) | `ambient-link-apple`; `ambient-link-meta/relay-ios` |
| `glass-link.ts` | **this dir** (no TS package yet) | `ambient-link-snapchat` (Lens Studio vendors the file) |
| `wear-data-layer.md` | **this dir** (protocol spec, language-agnostic) | `ambient-link-google/wear` |

Why TS still lives here: Lens Studio projects vendor `.ts` files rather than pull a
package, so `glass-link.ts` stays a copy-pasteable header until/unless a `core-ts`
npm package is worth it. The `.kt`/`.swift` copies that used to sit here have been
deleted — import the library types instead.

Rationale, perf patterns, and the repo reorg plan: see [`../ROUTING.md`](../ROUTING.md).
