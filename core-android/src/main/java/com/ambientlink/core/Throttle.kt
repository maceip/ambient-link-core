package com.ambientlink.core

/**
 * Per-key leading-edge rate gate. The first [allow] for a key passes; subsequent
 * calls within [intervalMillis] are dropped. Direct analogue of Cosmo's frame
 * gate (`if tsMillis - lastFrameAt < FRAME_PROCESS_INTERVAL_MS return`) and the
 * Go backpressure.Throttle.
 *
 * Use it to drop intermediate high-rate frames (camera frames, dictation
 * partials) while letting meaningful edges through; call [reset] on a boundary
 * (capture start / new turn) so the next frame is never delayed. An interval
 * <= 0 disables throttling.
 */
class Throttle(private val intervalMillis: Long) {
    private val last = HashMap<String, Long>()

    @Synchronized
    fun allow(key: String, atMillis: Long = System.currentTimeMillis()): Boolean {
        if (intervalMillis <= 0) return true
        val prev = last[key]
        if (prev != null && atMillis - prev < intervalMillis) return false
        last[key] = atMillis
        return true
    }

    @Synchronized
    fun reset(key: String) {
        last.remove(key)
    }
}
