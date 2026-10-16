import ComposableArchitecture
@preconcurrency import CoreBluetoothMock
import FCBComposable
import Foundation
import MeshProto
import MeshtasticKit
import MeshtasticMock
import Testing

struct SessionSendPreconditionTests {
    @Test func sendBeforeConnectThrowsNotConnected() async {
        let session = MeshtasticSession(central: .testValue)
        await #expect(throws: MeshtasticSessionError.notConnected) {
            _ = try await session.send(MeshtasticOutbound(portnum: 76, payload: Data([1])))
        }
    }
}

extension CBMBackedTests {
    @Suite @MainActor
    struct SessionSendTests {
        @Test func sendEncodesEveryOutboundField() async throws {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let payload = Data(repeating: 0xAB, count: 233)
            let id = try await h.session.send(MeshtasticOutbound(portnum: 76, payload: payload, to: 0x1234_5678,
                                                                 channel: 2, wantAck: true, hopLimit: 3))
            let sent = try #require(h.radio.device.receivedPackets.last)
            #expect(sent.id == id)
            #expect(sent.to == 0x1234_5678)
            #expect(sent.channel == 2)
            #expect(sent.wantAck == true)
            #expect(sent.hopLimit == 3)
            #expect(sent.decoded.portnum == .reticulumTunnelApp)
            #expect(sent.decoded.payload == payload)
        }

        @Test func nilHopLimitLeavesTheFieldUnsetForTheFirmwareDefault() async throws {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            _ = try await h.session.send(MeshtasticOutbound(portnum: 76, payload: Data([1])))
            let sent = try #require(h.radio.device.receivedPackets.last)
            #expect(sent.hopLimit == 0)
            #expect(sent.wantAck == false)
            #expect(sent.to == 0xFFFF_FFFF)
        }

        @Test func payloadOf234BytesThrowsAndWritesNothing() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            await #expect(throws: MeshtasticSessionError.payloadTooLarge) {
                _ = try await h.session.send(MeshtasticOutbound(portnum: 76, payload: Data(count: 234)))
            }
            #expect(h.radio.device.receivedPackets.isEmpty)
        }

        @Test func sendAfterDisconnectThrowsNotConnected() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            await h.session.disconnect()
            await #expect(throws: MeshtasticSessionError.notConnected) {
                _ = try await h.session.send(MeshtasticOutbound(portnum: 76, payload: Data([1])))
            }
        }

        @Test func generatedPacketIDZeroIsSentAsOne() async throws {
            let h = await makeSessionHarness(packetID: { 0 })
            await h.session.connect(radio: h.radio.id)
            let id = try await h.session.send(MeshtasticOutbound(portnum: 76, payload: Data([1])))
            #expect(id == 1)
            #expect(h.radio.device.receivedPackets.last?.id == 1)
        }

        @Test func port76SendIsAckedAsRoutingOk() async throws {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            let id = try await h.session.send(MeshtasticOutbound(portnum: 76, payload: Data([1])))
            let (seen, matched) = await collect(events) { $0.isRouting }
            #expect(matched)
            #expect(seen.last == .routing(packetID: id, ok: true))
        }

        @Test func nakedSendArrivesAsRoutingNotOk() async throws {
            let h = await makeSessionHarness()
            h.radio.device.ackBehavior = .nak(.noRoute)
            await h.session.connect(radio: h.radio.id)
            let events = h.session.events()
            let id = try await h.session.send(MeshtasticOutbound(portnum: 76, payload: Data([1])))
            let (seen, _) = await collect(events) { $0.isRouting }
            #expect(seen.last == .routing(packetID: id, ok: false))
        }

        /// FCB keeps ONE pending-write continuation per characteristic and
        /// overwrites it (BluetoothCentralClient+Live.swift:322-334): unserialised,
        /// concurrent sends leak all but the last. Unstructured tasks + polling so
        /// a hung send fails this test instead of hanging it.
        @Test func concurrentSendsAllCompleteWithDistinctIDs() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let results = LockIsolated<[UInt32]>([])
            for i in 0..<5 {
                Task { [session = h.session] in
                    if let id = try? await session.send(MeshtasticOutbound(portnum: 76, payload: Data([UInt8(i)]))) {
                        results.withValue { $0.append(id) }
                    }
                }
            }
            let allDone = await waitUntil(timeout: .seconds(5)) { results.value.count == 5 }
            #expect(allDone)
            #expect(results.value.count == 5)
            #expect(Set(results.value).count == 5)
            #expect(h.radio.device.receivedPackets.count == 5)
        }
    }
}
