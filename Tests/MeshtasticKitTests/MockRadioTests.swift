import ComposableArchitecture
@preconcurrency import CoreBluetoothMock
import FCBComposable
import Foundation
import MeshProto
import MeshtasticKit
import MeshtasticMock
import Testing

private struct RoutingReply: Equatable {
    var requestID: UInt32
    var errorReason: Routing.Error
}

private func routingReply(_ frame: FromRadio) -> RoutingReply? {
    guard case .packet(let packet)? = frame.payloadVariant,
          case .decoded(let data)? = packet.payloadVariant,
          data.portnum == .routingApp,
          let routing = try? Routing(serializedBytes: data.payload) else { return nil }
    return RoutingReply(requestID: data.requestID, errorReason: routing.errorReason)
}

private func packetToRadio(portnum: PortNum, id: UInt32, payload: Data = Data([0xAB])) -> ToRadio {
    var data = DataMessage()
    data.portnum = portnum
    data.payload = payload
    var packet = MeshPacket()
    packet.to = 0xFFFF_FFFF
    packet.id = id
    packet.decoded = data
    var toRadio = ToRadio()
    toRadio.packet = packet
    return toRadio
}

private func wantConfig(_ nonce: UInt32) -> ToRadio {
    var toRadio = ToRadio()
    toRadio.wantConfigID = nonce
    return toRadio
}

private func loraPresets(_ frames: [FromRadio]) -> [Config.LoRaConfig.ModemPreset] {
    frames.compactMap { frame in
        guard case .config(let config)? = frame.payloadVariant,
              case .lora(let lora)? = config.payloadVariant else { return nil }
        return lora.modemPreset
    }
}

extension CBMBackedTests {
    @Suite @MainActor
    struct MockRadioTests {
        @Test func handshakeServesLoRaConfigWithTheConfiguredPreset() async throws {
            let radio = await makeMockRadio(modemPreset: .shortFast)
            try await rawConnect(radio)
            try await rawWrite(wantConfig(42), radio)
            let frames = try await rawReadAll(radio)
            #expect(loraPresets(frames) == [.shortFast])
            guard case .configCompleteID(let nonce)? = frames.last?.payloadVariant else {
                Issue.record("last handshake frame must be config_complete_id")
                return
            }
            #expect(nonce == 42)
        }

        @Test func handshakeOmitsLoRaConfigWhenPresetIsNil() async throws {
            let radio = await makeMockRadio(modemPreset: nil)
            try await rawConnect(radio)
            try await rawWrite(wantConfig(7), radio)
            let frames = try await rawReadAll(radio)
            #expect(loraPresets(frames).isEmpty)
            #expect(!frames.isEmpty)
        }

        @Test func presetChangeIsServedOnTheNextHandshake() async throws {
            let radio = await makeMockRadio(modemPreset: .shortFast)
            try await rawConnect(radio)
            try await rawWrite(wantConfig(8), radio)
            #expect(loraPresets(try await rawReadAll(radio)) == [.shortFast])
            radio.device.modemPreset = .longModerate
            try await rawWrite(wantConfig(9), radio)
            #expect(loraPresets(try await rawReadAll(radio)) == [.longModerate])
        }

        @Test func packetOnAnyPortIsRoutingAcked() async throws {
            let radio = await makeMockRadio()
            try await rawConnect(radio)
            try await rawWrite(packetToRadio(portnum: .reticulumTunnelApp, id: 0x55), radio)
            let replies = try await rawReadAll(radio).compactMap(routingReply)
            #expect(replies == [RoutingReply(requestID: 0x55, errorReason: .none)])
        }

        @Test func nakBehaviorRepliesWithItsErrorReason() async throws {
            let radio = await makeMockRadio()
            radio.device.ackBehavior = .nak(.noRoute)
            try await rawConnect(radio)
            try await rawWrite(packetToRadio(portnum: .reticulumTunnelApp, id: 0x56), radio)
            let replies = try await rawReadAll(radio).compactMap(routingReply)
            #expect(replies == [RoutingReply(requestID: 0x56, errorReason: .noRoute)])
        }

        @Test func silentBehaviorSendsNoRoutingReply() async throws {
            let radio = await makeMockRadio()
            radio.device.ackBehavior = .silent
            try await rawConnect(radio)
            try await rawWrite(packetToRadio(portnum: .reticulumTunnelApp, id: 0x57), radio)
            #expect(try await rawReadAll(radio).compactMap(routingReply).isEmpty)
        }

        @Test func everyWrittenPacketIsRecordedInOrder() async throws {
            let radio = await makeMockRadio()
            try await rawConnect(radio)
            try await rawWrite(packetToRadio(portnum: .reticulumTunnelApp, id: 1, payload: Data([1])), radio)
            try await rawWrite(packetToRadio(portnum: .textMessageApp, id: 2, payload: Data([2])), radio)
            let recorded = radio.device.receivedPackets
            #expect(recorded.map(\.id) == [1, 2])
            #expect(recorded.map(\.decoded.portnum) == [.reticulumTunnelApp, .textMessageApp])
            #expect(recorded.map(\.decoded.payload) == [Data([1]), Data([2])])
        }

        @Test func injectedPacketIsReadableWithItsFields() async throws {
            let radio = await makeMockRadio()
            try await rawConnect(radio)
            radio.device.injectIncomingPacket(portnum: .reticulumTunnelApp, payload: Data([1, 2, 3]),
                                              from: 0xB001, to: 0xFFFF_FFFF, channel: 1, id: 9)
            let packets = try await rawReadAll(radio).compactMap { frame -> MeshPacket? in
                if case .packet(let packet)? = frame.payloadVariant { return packet }
                return nil
            }
            #expect(packets.count == 1)
            #expect(packets.first?.from == 0xB001)
            #expect(packets.first?.to == 0xFFFF_FFFF)
            #expect(packets.first?.channel == 1)
            #expect(packets.first?.id == 9)
            #expect(packets.first?.decoded.portnum == .reticulumTunnelApp)
            #expect(packets.first?.decoded.payload == Data([1, 2, 3]))
        }
    }
}
