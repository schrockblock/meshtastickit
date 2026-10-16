import Foundation
import os

/// Lock-backed fan-out behind `MeshtasticSession.events()`. Registration and the
/// connection-state replay happen synchronously inside `subscribe()`, so a
/// subscriber created before `connect` cannot miss that connect's first events
/// (an actor-hop registration, as in Meshtastified's old session, could land
/// after them).
final class EventHub: Sendable {
    private struct State: Sendable {
        var continuations: [UUID: AsyncStream<MeshtasticEvent>.Continuation] = [:]
        var connection: MeshtasticConnectionState = .disconnected(reason: nil)
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    var connection: MeshtasticConnectionState { state.withLock { $0.connection } }

    func subscribe() -> AsyncStream<MeshtasticEvent> {
        let token = UUID()
        return AsyncStream { continuation in
            state.withLock { s in
                s.continuations[token] = continuation
                continuation.yield(.connection(s.connection))
            }
            continuation.onTermination = { [weak self] _ in
                _ = self?.state.withLock { $0.continuations.removeValue(forKey: token) }
            }
        }
    }

    func emit(_ event: MeshtasticEvent) {
        state.withLock { s in
            for continuation in s.continuations.values { continuation.yield(event) }
        }
    }

    /// Stores and emits atomically, so the replayed value and the stream never disagree.
    func setConnection(_ connection: MeshtasticConnectionState) {
        state.withLock { s in
            s.connection = connection
            for continuation in s.continuations.values { continuation.yield(.connection(connection)) }
        }
    }
}
