package com.ambientlink.core

import kotlinx.coroutines.flow.StateFlow

/**
 * Vendor-neutral companion-capture contract. Implemented per device transport
 * (Android XR ProjectedGlassCaptureService, Meta DAT, …).
 *
 * Shape extracted from the recovered Cosmo CosmoGlassManager
 * (ambient-link-google/glasses_link.md). Now canonical here in core-android;
 * the per-repo copies (com.ambientlink.glasses.link, com.lowkey.ambientlink.link)
 * should depend on this.
 *
 * Impls MUST: expose state as StateFlow (no polled booleans), make bind()
 * idempotent, push media via callbacks, throttle frames (default 1/10s) through
 * an EphemeralBuffer, run capture in a typed foreground service, and honor a
 * settings gate before binding.
 */
interface GlassLink {
    val connected: StateFlow<Boolean>
    val bound: StateFlow<Boolean>

    suspend fun bind()
    fun unbind()

    fun setupImageCapture(onFrame: (Frame) -> Unit)
    fun startImageCapture()
    fun stopImageCapture()

    fun startAudioCapture(onBytes: (ByteArray, Int) -> Unit)
    fun stopAudioCapture()

    fun clear()

    data class Frame(val width: Int, val height: Int, val pixels: ByteArray, val tsMillis: Long)

    companion object {
        /** Cosmo: FRAME_PROCESS_INTERVAL_MS = 10_000 ms, GLASS_CAMERA_TARGET_FPS = 0.1. */
        const val DEFAULT_FRAME_INTERVAL_MS: Long = 10_000
    }
}
