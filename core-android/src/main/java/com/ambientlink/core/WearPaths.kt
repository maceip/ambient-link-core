package com.ambientlink.core

/**
 * Phone <-> watch Wearable Data Layer paths, modeled on Cosmo's `/cosmowear/...`
 * (glasses_link.md Link 2). Canonical spec:
 * ambient-link-core/contracts/wear-data-layer.md.
 *
 * Control/state ride messages; high-rate audio rides a dedicated channel with an
 * explicit stop. Shared so the watch + phone agree on the wire.
 */
object WearPaths {
    const val SESSIONS = "/ambientlink/sessions"               // phone -> watch (MessageClient)
    const val STATUS = "/ambientlink/status"                   // both (MessageClient)
    const val REPLY = "/ambientlink/reply"                     // watch -> phone (MessageClient)
    const val TRIGGER = "/ambientlink/trigger"                 // watch -> phone (MessageClient)
    const val MIC_STREAM = "/ambientlink/mic_stream"           // watch -> phone (ChannelClient)
    const val MIC_STREAM_STOP = "/ambientlink/mic_stream_stop" // watch -> phone (MessageClient)
}

/** Mirrors Cosmo CosmoPhoneStatus.Status (trimmed). */
enum class PhoneStatus { OFF, IDLE, LISTENING, PROCESSING, RESPONDING }

/** Mirrors Cosmo CosmoWatchStatus.Status (trimmed). */
enum class WatchStatus { OFF, STREAMING_AUDIO, DISCONNECTED }

/** Watch -> phone trigger types (mirrors Cosmo CosmoTrigger.ActionType intent). */
enum class TriggerType { NUDGE, OPEN, DICTATE_START, DICTATE_STOP }
