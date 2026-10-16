import Foundation
import MeshProto

/// One outbound Meshtastic data packet. `payload` is the DataMessage payload and
/// must be at most 233 bytes (Meshtastic's DATA_PAYLOAD_LEN); `MeshtasticSession.send`
/// throws `MeshtasticSessionError.payloadTooLarge` otherwise.
public struct MeshtasticOutbound: Equatable, Sendable {
    public var portnum: UInt32
    public var payload: Data            // ≤ 233 bytes; send throws .payloadTooLarge otherwise
    public var to: UInt32 = 0xFFFF_FFFF // broadcast
    public var channel: UInt32 = 0
    public var wantAck: Bool = false
    public var hopLimit: UInt32? = nil  // nil: firmware default

    public init(portnum: UInt32, payload: Data, to: UInt32 = 0xFFFF_FFFF, channel: UInt32 = 0,
                wantAck: Bool = false, hopLimit: UInt32? = nil) {
        self.portnum = portnum
        self.payload = payload
        self.to = to
        self.channel = channel
        self.wantAck = wantAck
        self.hopLimit = hopLimit
    }
}

/// `disconnected(reason:)` carries nil before any connect and after `disconnect()`;
/// otherwise one of the terminal causes "radio not found", "unauthorized", "unsupported".
/// `reconnecting(attempt:)` counts the attempt about to be made, starting at 1.
public enum MeshtasticConnectionState: Equatable, Sendable {
    case disconnected(reason: String?), connecting, configuring, connected, reconnecting(attempt: Int)
}

/// Why the Bluetooth adapter cannot be used. `isTerminal` separates causes retrying
/// cannot fix (`unauthorized`, `unsupported`) from transient ones.
public enum MeshtasticAdapterUnavailable: Equatable, Sendable {
    case poweredOff, unauthorized, unsupported, other(String?)

    public var isTerminal: Bool {
        switch self {
        case .unauthorized, .unsupported: return true
        case .poweredOff, .other: return false
        }
    }
}

/// Every MeshPacket also arrives as `.frame`: a consumer maps EITHER `.frame` OR
/// `.packet`/`.routing`, never both (spec §4 Contract 1).
public enum MeshtasticEvent: Equatable, Sendable {
    case connection(MeshtasticConnectionState)
    /// Emitted once per completed want_config handshake (so again after every reconnect).
    case configured(myNodeNum: UInt32, modemPreset: Int?)   // raw Config.LoRaConfig.ModemPreset value
    /// Every decoded MeshPacket, any port. Encrypted/undecodable packets are not emitted.
    case packet(from: UInt32, to: UInt32, channel: UInt32, portnum: UInt32, payload: Data, id: UInt32)
    case routing(packetID: UInt32, ok: Bool)
    /// Every decoded FromRadio frame, unfiltered — for Meshtastified's node DB / channels / text.
    case frame(FromRadio)
    /// `.poweredOff` while waiting for power; `.unauthorized` / `.unsupported` are terminal
    /// (the session then ends `.disconnected(reason: "unauthorized" / "unsupported")`).
    case adapterUnavailable(MeshtasticAdapterUnavailable)
}

public struct DiscoveredRadio: Equatable, Sendable, Identifiable {
    public var id: UUID
    public var name: String?
    public var rssi: Int

    public init(id: UUID, name: String?, rssi: Int) {
        self.id = id
        self.name = name
        self.rssi = rssi
    }
}

public enum MeshtasticSessionError: Error, Equatable, Sendable {
    /// `MeshtasticOutbound.payload` exceeded 233 bytes; nothing was written.
    case payloadTooLarge
    /// `send` was called while the session was not `.connected`.
    case notConnected
}
