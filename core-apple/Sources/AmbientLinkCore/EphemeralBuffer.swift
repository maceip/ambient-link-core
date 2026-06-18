import Foundation

/// Bounded, time-expiring buffer for captured media. Swift peer of the Go
/// `backpressure.EphemeralBuffer` and the Kotlin `core-android` one — mirrors
/// Cosmo's `InMemoryEphemeralBuffer` (glasses_link.md): items live for a bounded
/// window and are then evicted, so memory stays flat under continuous capture.
public final class EphemeralBuffer<T>: @unchecked Sendable {
    private struct Stamped { let item: T; let ts: TimeInterval }

    public let ttl: TimeInterval
    private let maxItems: Int
    private let clock: @Sendable () -> TimeInterval
    private var items: [Stamped] = []
    private let lock = NSLock()

    public init(ttl: TimeInterval = 60,
                maxItems: Int = 64,
                clock: @escaping @Sendable () -> TimeInterval = { Date().timeIntervalSince1970 }) {
        self.ttl = ttl
        self.maxItems = maxItems
        self.clock = clock
    }

    public func add(_ item: T, at ts: TimeInterval? = nil) {
        lock.lock(); defer { lock.unlock() }
        items.append(Stamped(item: item, ts: ts ?? clock()))
        if items.count > maxItems { items.removeFirst(items.count - maxItems) }
        evictLocked()
    }

    public func snapshot() -> [T] {
        lock.lock(); defer { lock.unlock() }
        evictLocked()
        return items.map(\.item)
    }

    public func clear() {
        lock.lock(); defer { lock.unlock() }
        items.removeAll()
    }

    private func evictLocked() {
        let cutoff = clock() - ttl
        if let idx = items.firstIndex(where: { $0.ts >= cutoff }) {
            if idx > 0 { items.removeFirst(idx) }
        } else if !items.isEmpty {
            items.removeAll()
        }
    }
}
