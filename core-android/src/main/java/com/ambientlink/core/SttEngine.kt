package com.ambientlink.core

/**
 * Vendor-neutral streaming speech-to-text contract for HUD dictation.
 *
 * One uniform surface over whatever on-device recognizer a platform ships:
 *  - Meta: SODA (recovered Google binary) — `SodaDictationEngine` is the reference
 *    impl and conforms to this shape (its native `.jar`/`.so` are not redistributable,
 *    so the engine stays in ambient-link-meta rather than this shared lib).
 *  - Google / Wear: Android `SpeechRecognizer` or the same SODA pack.
 *  - Apple: `Speech` / on-device dictation (see core-apple).
 *
 * The lifecycle mirrors the relay dictation protocol (begin → partial* → commit),
 * so an impl feeds [Callbacks] straight into `dictate_*` frames. Rate-limit
 * `onPartial` with [Throttle] before fan-out (the Go relay already collapses
 * `dictate_partial` bursts; do the same at the edge to save power).
 */
interface SttEngine {
    /** Begin mic capture + recognition. Idempotent: a second call while running is a no-op. */
    fun start()

    /**
     * Stop capture/recognition. If [commitPartial] and a trailing partial exists,
     * the impl should emit it as a final via [Callbacks.onFinal] before ending.
     */
    fun stop(commitPartial: Boolean = true)

    /** Most recent uncommitted partial transcript, or "" if none. */
    fun lastPartialText(): String

    interface Callbacks {
        /** Interim hypothesis; high-frequency — throttle before sending upstream. */
        fun onPartial(text: String)
        /** Stable transcript for the current utterance. */
        fun onFinal(text: String)
        /** Terminal failure (e.g. "soda_unavailable", "mic_start_failed"). */
        fun onError(reason: String)
        /** Human-readable progress ("preparing…", "listening…"). */
        fun onStatus(message: String)
        /** Recognizer closed its session (optional). */
        fun onSessionEnded() {}
    }
}
