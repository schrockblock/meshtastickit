import ComposableArchitecture
@preconcurrency import CoreBluetoothMock
import FCBComposable
import Foundation
import MeshProto
import MeshtasticKit
import MeshtasticMock
import Testing

private func routingFrame(requestID: UInt32, reason: Routing.Error) throws -> FromRadio {
    var routing = Routing()
    routing.errorReason = reason
    var data = DataMessage()
    data.portnum = .routingApp
    data.requestID = requestID
    data.payload = try routing.serializedData()
    var packet = MeshPacket()
    packet.from = MockMeshtasticDevice.selfNodeNum
    packet.decoded = data
    var frame = FromRadio()
    frame.packet = packet
    return frame
}

extension CBMBackedTests {
    @Suite @MainActor
    struct SessionReceiveTests {
        @Test func incomingPort76PacketEmitsPacketWithEveryField() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            h.radio.device.injectIncomingPacket(portnum: .reticulumTunnelApp, payload: Data([1, 2, 3]),
                                                from: 0xB001, to: 0xFFFF_FFFF, channel: 2, id: 0x1234)
            let (seen, matched) = await collect(events) { $0.packetID != nil }
            #expect(matched)
            #expect(seen.last == .packet(from: 0xB001, to: 0xFFFF_FFFF, channel: 2, portnum: 76,
                                                           payload: Data([1, 2, 3]), id: 0x1234))
        }

        @Test func frameForAPacketPrecedesItsPacketEvent() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            h.radio.device.injectIncomingPacket(portnum: .reticulumTunnelApp, payload: Data([7]), from: 0xB002, id: 0x77)
            let (seen, _) = await collect(events) { $0.packetID != nil }
            let tail = Array(seen.suffix(2))
            #expect(tail.count == 2)
            guard case .packet(let packet)? = tail.first?.frameValue?.payloadVariant else {
                Issue.record("the event before .packet must be its .frame")
                return
            }
            #expect(packet.id == 0x77)
        }

        @Test func nonPacketFrameIsEmittedAsFrame() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            var frame = FromRadio()
            frame.rebooted = true
            h.radio.device.injectFrame(frame)
            let (_, matched) = await collect(events) {
                if case .rebooted(true)? = $0.frameValue?.payloadVariant { return true }
                return false
            }
            #expect(matched)
        }

        @Test func steadyStateConfigFrameDoesNotReemitConfigured() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            var lora = Config.LoRaConfig()
            lora.modemPreset = .longSlow
            var config = Config()
            config.lora = lora
            var frame = FromRadio()
            frame.config = config
            h.radio.device.injectFrame(frame)
            h.radio.device.injectIncomingPacket(portnum: .reticulumTunnelApp, payload: Data([1]), from: 0xB001, id: 5)
            let (seen, matched) = await collect(events) { $0.packetID == 5 }
            #expect(matched)
            #expect(!seen.contains { $0.isConfigured })
        }

        @Test func routingAckEmitsRoutingOk() async throws {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            h.radio.device.injectFrame(try routingFrame(requestID: 77, reason: .none))
            let (seen, matched) = await collect(events) { $0.isRouting }
            #expect(matched)
            #expect(seen.last == .routing(packetID: 77, ok: true))
        }

        @Test func routingErrorEmitsRoutingNotOk() async throws {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            h.radio.device.injectFrame(try routingFrame(requestID: 78, reason: .noRoute))
            let (seen, _) = await collect(events) { $0.isRouting }
            #expect(seen.last == .routing(packetID: 78, ok: false))
        }

        @Test func encryptedPacketEmitsFrameButNoPacket() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            var encrypted = MeshPacket()
            encrypted.from = 0xB003
            encrypted.id = 0xE1
            encrypted.encrypted = Data([0xDE, 0xAD])
            var frame = FromRadio()
            frame.packet = encrypted
            h.radio.device.injectFrame(frame)
            h.radio.device.injectIncomingPacket(portnum: .reticulumTunnelApp, payload: Data([1]), from: 0xB001, id: 0xE2)
            let (seen, matched) = await collect(events) { $0.packetID == 0xE2 }
            #expect(matched)
            #expect(seen.compactMap(\.packetID) == [0xE2])
            #expect(seen.contains { event in
                if case .packet(let p)? = event.frameValue?.payloadVariant { return p.id == 0xE1 }
                return false
            })
        }

        @Test func negativePortnumIsDroppedAndTheDrainContinues() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            var data = DataMessage()
            data.portnum = .UNRECOGNIZED(-1)
            data.payload = Data([1])
            var bad = MeshPacket()
            bad.from = 0xB004
            bad.id = 0xBAD
            bad.decoded = data
            var frame = FromRadio()
            frame.packet = bad
            h.radio.device.injectFrame(frame)
            h.radio.device.injectIncomingPacket(portnum: .reticulumTunnelApp, payload: Data([2]), from: 0xB001, id: 0x600D)
            let (seen, matched) = await collect(events) { $0.packetID == 0x600D }
            #expect(matched)
            #expect(seen.compactMap(\.packetID) == [0x600D])
        }

        @Test func garbageFrameIsSkippedAndTheDrainContinues() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            // Truncated varint: not a FromRadio. Queued WITHOUT a FromNum so the good
            // packet's notify drains both in ONE drain — the garbage must not end it.
            h.radio.device.injectRawFrame(Data([0xFF, 0xFF, 0xFF]), notify: false)
            h.radio.device.injectIncomingPacket(portnum: .reticulumTunnelApp, payload: Data([3]), from: 0xB001, id: 0x600E)
            let (_, matched) = await collect(events) { $0.packetID == 0x600E }
            #expect(matched)
        }
    }
}
