package com.ambientlink.core

/**
 * A single coding-agent session as reported by the Ambient Link relay.
 *
 * Canonical model shared by every Android consumer (glasses app, Wear watch).
 * Previously duplicated in com.ambientlink.glasses.data and
 * com.ambientlink.watch.data — those copies are removed in favor of this.
 */
data class Session(
    val sessionId: String,
    val agent: String,      // "cursor" | "claude" | "codex"
    val cwd: String,
    val state: String,      // "BUSY" | "IDLE" | "DEAD"
    val preview: String = "",
) {
    val isLive: Boolean get() = state != "DEAD"

    /** Last path component of cwd, for compact wrist/HUD labels. */
    val shortCwd: String get() = cwd.substringAfterLast('/').ifBlank { cwd }

    val label: String get() = "$agent: $shortCwd"
}
