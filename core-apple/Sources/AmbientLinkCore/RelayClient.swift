import Foundation
import Observation

/// Talks to the Ambient Link relay (the same one the glasses + web app use).
///
/// Read:  `GET  {base}/ambient-link/status`   -> { sessions: [...] }
/// Reply: `POST {base}/ambient-link/ingest`   (quick reply routed to a session)
public struct RelayClient: Sendable {
    public let baseURL: URL
    private let session: URLSession

    public init(baseURL: URL = URL(string: "https://public.computer")!,
                session: URLSession = .shared) {
        self.baseURL = baseURL
        self.session = session
    }

    public func fetchSessions() async throws -> [Session] {
        let url = baseURL.appendingPathComponent("ambient-link/status")
        let (data, response) = try await session.data(from: url)
        guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
            throw RelayError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
        return try Session.list(from: data)
    }

    /// Send a quick reply / nudge to an agent session via the relay ingest endpoint.
    public func reply(to sessionId: String, text: String) async throws {
        let url = baseURL.appendingPathComponent("ambient-link/ingest")
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body: [String: Any] = [
            "source": "apple",
            "session_id": sessionId,
            "event_type": "user_reply",
            "payload": ["message": text],
            "observed_at": Int(Date().timeIntervalSince1970 * 1000),
        ]
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (_, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse, (200...299).contains(http.statusCode) else {
            throw RelayError.badStatus((response as? HTTPURLResponse)?.statusCode ?? -1)
        }
    }

    public enum RelayError: Error, Sendable {
        case badStatus(Int)
    }
}

/// Observable polling store for SwiftUI (visionOS / iOS / watchOS).
///
/// `@Observable` lets any SwiftUI view read `sessions` and re-render on change.
@MainActor
@Observable
public final class SessionStore {
    public private(set) var sessions: [Session] = []
    public private(set) var lastError: String?

    private let client: RelayClient
    private let pollInterval: Duration
    private var pollTask: Task<Void, Never>?

    public init(client: RelayClient = RelayClient(), pollInterval: Duration = .seconds(5)) {
        self.client = client
        self.pollInterval = pollInterval
    }

    public var live: [Session] { sessions.filter(\.isLive) }

    public func start() {
        guard pollTask == nil else { return }
        pollTask = Task { [weak self] in
            guard let self else { return }
            while !Task.isCancelled {
                await self.refresh()
                try? await Task.sleep(for: self.pollInterval)
            }
        }
    }

    public func stop() {
        pollTask?.cancel()
        pollTask = nil
    }

    public func refresh() async {
        do {
            sessions = try await client.fetchSessions()
            lastError = nil
        } catch {
            lastError = String(describing: error)
        }
    }

    public func reply(to session: Session, text: String) async {
        try? await client.reply(to: session.sessionId, text: text)
    }
}
