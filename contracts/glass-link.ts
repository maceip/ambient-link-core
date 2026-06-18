// CANONICAL CONTRACT — copy into ambient-link-snapchat (Lens Studio) and implement
// against the Spectacles transport. No vendor types here.
//
// Extracted from the recovered Cosmo CosmoGlassManager shape
// (see ambient-link-google/glasses_link.md). Same boundary, TS idioms.
//
// Implementations MUST: surface state via observable getters/events (no polled
// booleans), make bind() idempotent, push media via callbacks, throttle frames
// (default 1 / 10s) through an EphemeralBuffer, and honor a settings gate.

/** Vendor-neutral frame envelope. */
export interface GlassFrame {
  width: number;
  height: number;
  pixels: Uint8Array;
  tsMillis: number;
}

export type FrameSink = (frame: GlassFrame) => void;
export type AudioSink = (bytes: Uint8Array, validLen: number) => void;

export interface GlassLink {
  /** Device present/reachable. */
  readonly connected: boolean;
  /** Capture session live. */
  readonly bound: boolean;
  /** Subscribe to state transitions (no polling). */
  onState(listener: (s: { connected: boolean; bound: boolean }) => void): () => void;

  /** Idempotent; honors the per-link settings gate. */
  bind(): Promise<void>;
  unbind(): void;

  setupImageCapture(onFrame: FrameSink): void;
  startImageCapture(): void;
  stopImageCapture(): void;

  startAudioCapture(onBytes: AudioSink): void;
  stopAudioCapture(): void;

  clear(): void;
}

/** Cosmo: 10s frame interval / 0.1 fps target. */
export const DEFAULT_FRAME_INTERVAL_MS = 10_000;

/** TTL ring buffer for captured media. Mirrors Cosmo's InMemoryEphemeralBuffer. */
export interface EphemeralBuffer<T> {
  readonly ttlMillis: number;
  add(item: T, tsMillis: number): void;
  snapshot(): T[];
  clear(): void;
}
