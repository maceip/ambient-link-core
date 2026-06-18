import Foundation

// CANONICAL CONTRACT — copy into ambient-link-apple (and meta-ios) and implement
// against the real transport (RealityKit / audio engine / DAT). No vendor types here.
//
// Extracted from the recovered Cosmo CosmoGlassManager shape
// (see ambient-link-google/glasses_link.md). Same boundary, Swift idioms.
//
// Implementations MUST: expose state via AsyncStream/Observation (no polled bools),
// make bind() idempotent, push media via callbacks, throttle frames (default 1/10s)
// through an EphemeralBuffer, run capture in a sanctioned background mode, and honor
// a settings gate before binding.

/// Vendor-neutral frame envelope.
public struct GlassFrame: Sendable {
    public let width: Int
    public let height: Int
    public let pixels: Data
    public let timestamp: Date
    public init(width: Int, height: Int, pixels: Data, timestamp: Date) {
        self.width = width; self.height = height; self.pixels = pixels; self.timestamp = timestamp
    }
}

public protocol GlassLink: AnyObject, Sendable {
    /// Device present/reachable.
    var connected: AsyncStream<Bool> { get }
    /// Capture session live.
    var bound: AsyncStream<Bool> { get }

    /// Idempotent; honors the per-link settings gate.
    func bind() async
    func unbind()

    func setupImageCapture(onFrame: @escaping @Sendable (GlassFrame) -> Void)
    func startImageCapture()
    func stopImageCapture()

    /// Audio bytes + valid length, fed into the shared STT pipeline.
    func startAudioCapture(onBytes: @escaping @Sendable (Data, Int) -> Void)
    func stopAudioCapture()

    func clear()
}

public enum GlassLinkDefaults {
    /// Cosmo: 10s frame interval / 0.1 fps target.
    public static let frameIntervalMillis: Int = 10_000
}

/// TTL ring buffer for captured media. Mirrors Cosmo's InMemoryEphemeralBuffer.
public protocol EphemeralBuffer {
    associatedtype Element
    var ttl: Duration { get }
    func add(_ item: Element, at timestamp: Date)
    func snapshot() -> [Element]
    func clear()
}
