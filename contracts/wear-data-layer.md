# Wear data-layer protocol (`/ambientlink/...`)

Phone <-> watch protocol for the Wear OS companion, modeled directly on Cosmo's
`/cosmowear/...` Wearable Data Layer design (NodeClient / MessageClient /
ChannelClient). Use this for the **phone-tethered** path; the standalone path may
hit the relay directly over Wi-Fi/LTE.

Why mirror Cosmo: messages for low-rate control/state, a dedicated **channel** for
high-rate audio with an **explicit stop** — this is what keeps the wrist battery-safe.

## Paths

| Path | Direction | Mechanism | Payload |
|---|---|---|---|
| `/ambientlink/sessions` | phone -> watch | `MessageClient.sendMessage` | session list snapshot (compact JSON or proto) |
| `/ambientlink/status` | both | `MessageClient` | phone/watch status enum |
| `/ambientlink/reply` | watch -> phone | `MessageClient` | `{ sessionId, text }` quick reply -> relay ingest |
| `/ambientlink/trigger` | watch -> phone | `MessageClient` | `{ type: NUDGE \| OPEN \| DICTATE_START \| DICTATE_STOP }` |
| `/ambientlink/mic_stream` | watch -> phone | `ChannelClient.Channel` | raw mic audio -> shared STT |
| `/ambientlink/mic_stream_stop` | watch -> phone | `MessageClient` | control: stop the audio channel |

## Status enums (mirror Cosmo's CosmoPhoneStatus / CosmoWatchStatus)

```
PhoneStatus:  OFF | IDLE | LISTENING | PROCESSING | RESPONDING
WatchStatus:  OFF | STREAMING_AUDIO | DISCONNECTED
```

## Rules

- Control/state over **messages**; audio over a **channel** opened on demand and
  closed on `/ambientlink/mic_stream_stop`.
- The mic channel is owned by a **dataSync foreground service** on the phone
  (mirrors `WearableAudioStreamService`), feeding the same STT sink as glasses mic.
- All watch state is reactive; the watch UI never polls when tethered.
- Keep these paths stable — they are part of the contract, like `/cosmowear/...`.
