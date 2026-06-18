package com.ambientlink.contracts

import kotlinx.coroutines.flow.StateFlow

/**
 * CANONICAL CONTRACT — copy into each vendor repo and implement against the real
 * device transport. Do not add vendor types here.
 *
 * Extracted from the recovered Cosmo `CosmoGlassManager` shape
 * (see ambient-link-google/glasses_link.md). The transport below this interface is
 * always vendor-hidden; this is the stable boundary above it.
 *
 * Implementations MUST:
 *  - expose link state only as StateFlow (no polled booleans),
 *  - make bind() idempotent (no-op if bound or binding in flight),
 *  - push media through callbacks (frames/audio), never block,
 *  - throttle frames (default 1 / 10s) and route media through an EphemeralBuffer,
 *  - run capture inside a typed foreground service,
 *  - honor a settings gate before binding.
 */
interface GlassLink {
    /** Device present/reachable (e.g. ProjectedContext.isProjectedDeviceConnected). */
    val connected: StateFlow<Boolean>

    /** Capture service bound / session live. */
    val bound: StateFlow<Boolean>

    /** Idempotent. Honors the per-link settings gate; no-op when already (un)bound. */
    suspend fun bind()
    fun unbind()

    /** Register the frame sink before starting image capture. */
    fun setupImageCapture(onFrame: (Frame) -> Unit)
    fun startImageCapture()
    fun stopImageCapture()

    /** Audio bytes + valid length, fed into the shared STT pipeline. */
    fun startAudioCapture(onBytes: (ByteArray, Int) -> Unit)
    fun stopAudioCapture()

    /** Drop buffers and reset all gates. */
    fun clear()

    /** Vendor-neutral frame envelope; impls wrap their native bitmap/proxy. */
    data class Frame(val width: Int, val height: Int, val pixels: ByteArray, val tsMillis: Long)

    companion object {
        /** Cosmo: FRAME_PROCESS_INTERVAL_MS = 10_000 ms, GLASS_CAMERA_TARGET_FPS = 0.1. */
        const val DEFAULT_FRAME_INTERVAL_MS: Long = 10_000
    }
}

/**
 * TTL ring buffer for captured media. Mirrors Cosmo's InMemoryEphemeralBuffer +
 * getEphemeralBufferDurationMin: bounded, time-expiring, clearable. Keeps memory
 * flat under continuous capture.
 */
interface EphemeralBuffer<T> {
    fun add(item: T, tsMillis: Long)
    fun snapshot(): List<T>
    fun clear()
    /** Items older than this are evicted on add()/snapshot(). */
    val ttlMillis: Long
}
