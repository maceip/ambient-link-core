import Foundation

/// A single coding-agent session as reported by the Ambient Link relay.
///
/// Same shape the glasses + web surfaces read from
/// `GET {base}/ambient-link/status`. Canonical Apple model — previously copied in
/// ambient-link-apple's AmbientLinkKit and ambient-link-meta/relay-ios.
public struct Session: Identifiable, Sendable, Codable, Hashable {
    public enum State: String, Sendable, Codable {
        case busy = "BUSY"
        case idle = "IDLE"
        case dead = "DEAD"
    }

    public let sessionId: String
    public let agent: String          // "cursor" | "claude" | "codex"
    public let cwd: String
    public let state: State
    public let preview: String

    public var id: String { sessionId }
    public var isLive: Bool { state != .dead }

    /// Last path component of the working directory, for compact labels.
    public var shortCwd: String {
        let trimmed = cwd.split(separator: "/").last.map(String.init)
        return trimmed?.isEmpty == false ? trimmed! : cwd
    }

    public var label: String { "\(agent): \(shortCwd)" }

    public init(sessionId: String, agent: String, cwd: String, state: State, preview: String = "") {
        self.sessionId = sessionId
        self.agent = agent
        self.cwd = cwd
        self.state = state
        self.preview = preview
    }
}

extension Session {
    /// Decodes the relay's `{ "sessions": [...] }` status payload.
    struct StatusPayload: Decodable {
        let sessions: [RawSession]
        struct RawSession: Decodable {
            let session_id: String?
            let agent: String?
            let cwd: String?
            let state: String?
            let preview: String?
        }
    }

    static func list(from data: Data) throws -> [Session] {
        let payload = try JSONDecoder().decode(StatusPayload.self, from: data)
        return payload.sessions.map { r in
            Session(
                sessionId: r.session_id ?? UUID().uuidString,
                agent: r.agent ?? "agent",
                cwd: r.cwd ?? "",
                state: Session.State(rawValue: r.state ?? "IDLE") ?? .idle,
                preview: r.preview ?? ""
            )
        }
    }
}
