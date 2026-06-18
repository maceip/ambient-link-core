import Foundation

/// Vendor-neutral companion-capture contract for Apple platforms. Canonical here;
/// ambient-link-apple and ambient-link-meta/relay-ios depend on this instead of
/// carrying copies.
///
/// visionOS has no projected capture service like Cosmo's, but the boundary still
/// applies: a GlassLink impl can wrap the audio engine + relay so the Siri/App
/// Intents layer and the RealityKit frontend consume one uniform surface. Shape
/// extracted from the recovered Cosmo CosmoGlassManager
/// (ambient-link-google/glasses_link.md); plan in ambient-link-core/ROUTING.md.
public struct GlassFrame: Sendable {
    public let width: Int
    public let height: Int
    public let pixels: Data
    public let timestamp: Date
    public init(width: Int, height: Int, pixels: Data, timestamp: Date) {
        self.width = width; self.height = height; self.pixels = pixels; self.timestamp = timestamp
    }
}

public protocol GlassLink: AnyObject {
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
