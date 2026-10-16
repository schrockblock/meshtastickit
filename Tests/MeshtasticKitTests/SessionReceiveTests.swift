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

        /// A packet that reaches the radio's FromRadio queue during the want_config
        /// handshake lands BEHIND config_complete (the queue is FIFO), so the
        /// handshake stops before it. Queued with no FromNum notify, as firmware
        /// queues packets heard while no phone was connected: only a drain after
        /// `.configured` can deliver it. The hook queues it inside the FIRST
        /// handshake read — deterministic.
        @Test func packetQueuedDuringHandshakeIsDeliveredAfterConfigured() async throws {
            let h = await makeSessionHarness()
            var data = DataMessage()
            data.portnum = .reticulumTunnelApp
            data.payload = Data([0x42])
            var packet = MeshPacket()
            packet.from = 0xB001
            packet.to = 0xFFFF_FFFF
            packet.id = 0x0D0E
            packet.decoded = data
            var frame = FromRadio()
            frame.packet = packet
            let bytes = try frame.serializedData()
            let armed = LockIsolated(true)
            h.radio.device.onFromRadioRead = { [device = h.radio.device] _ in
                let fire = armed.withValue { isArmed -> Bool in defer { isArmed = false }; return isArmed }
                if fire { device.injectRawFrame(bytes, notify: false) }
            }
            let events = h.session.events()
            await h.session.connect(radio: h.radio.id)
            let (seen, matched) = await collect(events, timeout: .seconds(2)) { $0.packetID == 0x0D0E }
            #expect(!armed.value)
            #expect(matched)
            let configuredAt = seen.firstIndex { $0.isConfigured }
            let packetAt = seen.firstIndex { $0.packetID == 0x0D0E }
            #expect(configuredAt != nil && packetAt != nil && configuredAt! < packetAt!)
            h.radio.device.onFromRadioRead = nil
            await h.session.disconnect()
        }

        /// A packet (with its FromNum notify) that arrives while the POST-CONFIGURE
        /// drain's read is in flight: that read was already served empty, so the drain
        /// misses it, and its notify finds the drain lock held. (Notify-triggered
        /// drains are serialised by the FromNum loop, so only the handshake and the
        /// post-configure drain can hold the lock when a notify arrives.) The notify
        /// must not be dropped. The hook queues the packet inside that empty read.
        @Test func notifyArrivingDuringThePostConfigureDrainIsNotLost() async {
            let h = await makeSessionHarness()
            // 0: handshake; 1: config_complete served, the next empty read is the
            // post-configure drain's; 2: the packet was queued inside that read.
            let phase = LockIsolated(0)
            h.radio.device.onFromRadioRead = { [device = h.radio.device] served in
                let isConfigComplete: Bool = {
                    guard let frame = try? FromRadio(serializedBytes: served),
                          case .configCompleteID? = frame.payloadVariant else { return false }
                    return true
                }()
                let inject = phase.withValue { p -> Bool in
                    if p == 0, isConfigComplete { p = 1; return false }
                    guard p == 1, served.isEmpty else { return false }
                    p = 2
                    return true
                }
                if inject {
                    device.injectIncomingPacket(portnum: .reticulumTunnelApp, payload: Data([2]), from: 0xB002, id: 0x0B0B)
                    // CBM schedules this read's (empty) response only after the hook
                    // returns, while the notify is already scheduled: holding here
                    // makes the notify reach the session while the drain still waits
                    // on this read (a 50 ms margin, not strict ordering).
                    Thread.sleep(forTimeInterval: 0.05)
                }
            }
            let events = h.session.events()
            await h.session.connect(radio: h.radio.id)
            let (_, matched) = await collect(events, timeout: .seconds(2)) { $0.packetID == 0x0B0B }
            #expect(phase.value == 2)   // the scenario: queued inside the post-configure drain's read
            #expect(matched)
            h.radio.device.onFromRadioRead = nil
            await h.session.disconnect()
        }
    }
}
