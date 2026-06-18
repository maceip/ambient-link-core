import Foundation

/// Per-key leading-edge rate gate. The first `allow` for a key passes; subsequent
/// calls within `interval` are dropped. Swift peer of the Go `backpressure.Throttle`
/// and the Kotlin `core-android` one — analogue of Cosmo's frame gate
/// (`if ts - lastFrameAt < FRAME_PROCESS_INTERVAL_MS return`).
///
/// Call `reset` on a boundary (capture start / new turn) so the next frame is never
/// delayed. An interval <= 0 disables throttling.
public final class Throttle: @unchecked Sendable {
    private let interval: TimeInterval
    private var last: [String: TimeInterval] = [:]
    private let lock = NSLock()

    public init(interval: TimeInterval) {
        self.interval = interval
    }

    public func allow(_ key: String, at now: TimeInterval = Date().timeIntervalSince1970) -> Bool {
        if interval <= 0 { return true }
        lock.lock(); defer { lock.unlock() }
        if let prev = last[key], now - prev < interval { return false }
        last[key] = now
        return true
    }

    public func reset(_ key: String) {
        lock.lock(); defer { lock.unlock() }
        last[key] = nil
    }
}
