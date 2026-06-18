import Testing
import Foundation
@testable import AmbientLinkCore

@Suite struct SessionDecoding {
    @Test func parsesRelayStatusPayload() throws {
        let json = """
        { "sessions": [
            { "session_id": "a1", "agent": "claude", "cwd": "/Users/me/proj", "state": "BUSY", "preview": "thinking" },
            { "session_id": "b2", "agent": "codex", "cwd": "/Users/me/other", "state": "DEAD" }
        ] }
        """.data(using: .utf8)!

        let sessions = try Session.list(from: json)
        #expect(sessions.count == 2)
        #expect(sessions[0].agent == "claude")
        #expect(sessions[0].state == .busy)
        #expect(sessions[0].shortCwd == "proj")
        #expect(sessions[0].isLive == true)
        #expect(sessions[1].isLive == false)
    }

    @Test func toleratesMissingFields() throws {
        let json = #"{ "sessions": [ { "agent": "cursor" } ] }"#.data(using: .utf8)!
        let sessions = try Session.list(from: json)
        #expect(sessions.count == 1)
        #expect(sessions[0].state == .idle)
    }
}

@Suite struct BackpressurePrimitives {
    @Test func throttleCollapsesBurstAndResets() {
        let t = Throttle(interval: 0.15)
        #expect(t.allow("k", at: 0) == true)
        #expect(t.allow("k", at: 0.05) == false)
        #expect(t.allow("k", at: 0.20) == true)
        t.reset("k")
        #expect(t.allow("k", at: 0.21) == true)
    }

    @Test func ephemeralBufferEvictsByTTL() {
        final class Clock: @unchecked Sendable { var t: TimeInterval = 0 }
        let clk = Clock()
        let b = EphemeralBuffer<Int>(ttl: 1.0, clock: { clk.t })

        clk.t = 0;   b.add(1)
        clk.t = 0.5; b.add(2)
        clk.t = 0.5; #expect(b.snapshot() == [1, 2])
        clk.t = 1.2; #expect(b.snapshot() == [2])   // item@0 is older than cutoff 0.2
        b.clear()
        #expect(b.snapshot().isEmpty)
    }
}
