import ComposableArchitecture
@preconcurrency import CoreBluetooth
@preconcurrency import CoreBluetoothMock
import FCBComposable
import Foundation
import MeshProto
import MeshtasticKit
import MeshtasticMock
import Testing

struct SessionReplayTests {
    @Test func eventsReplayDisconnectedBeforeAnyConnect() async {
        let session = MeshtasticSession(central: .testValue)
        let (events, matched) = await collect(session.events(), timeout: .seconds(2)) { _ in true }
        #expect(matched)
        #expect(events.first?.connectionState == .disconnected(reason: nil))
    }

    @Test func unauthorizedAdapterIsTerminalAndNeverConnects() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let session = MeshtasticSession(central: fakeCentral(radio: id, state: .unauthorized, log: log, connect: {}),
                                        clock: ImmediateClock())
        let events = session.events()
        let connecting = Task { await session.connect(radio: id) }
        let (seen, matched) = await collect(events) { $0.connectionState == .disconnected(reason: "unauthorized") }
        #expect(matched)
        #expect(seen.filter(\.isAdapterUnavailable) == [.adapterUnavailable(.unauthorized)])
        #expect(!log.value.contains("connect"))
        await session.disconnect()
        connecting.cancel()
    }
}

extension CBMBackedTests {
    @Suite @MainActor
    struct SessionConnectTests {
        @Test func connectRunsHandshakeAndEmitsConfiguredWithThePreset() async {
            let h = await makeSessionHarness(modemPreset: .shortFast)
            let events = h.session.events()
            await h.session.connect(radio: h.radio.id)
            let (seen, matched) = await collect(events) { $0.isConfigured }
            #expect(matched)
            #expect(seen.compactMap(\.connectionState) == [.disconnected(reason: nil), .connecting, .configuring, .connected])
            #expect(seen.last == .configured(myNodeNum: MockMeshtasticDevice.selfNodeNum,
                                             modemPreset: Config.LoRaConfig.ModemPreset.shortFast.rawValue))
            let nodeFrames = seen.compactMap(\.frameValue).filter {
                if case .nodeInfo? = $0.payloadVariant { return true }
                return false
            }
            #expect(nodeFrames.count == 7)
        }

        @Test func configuredReportsNilPresetWhenTheRadioSendsNoLoRaConfig() async {
            let h = await makeSessionHarness(modemPreset: nil)
            let events = h.session.events()
            await h.session.connect(radio: h.radio.id)
            let (seen, _) = await collect(events) { $0.isConfigured }
            #expect(seen.last == .configured(myNodeNum: MockMeshtasticDevice.selfNodeNum, modemPreset: nil))
        }

        @Test func connectRetrievesThePeripheralFirstAndNeverScans() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            #expect(h.log.value == ["retrievePeripherals", "connect"])
        }

        @Test func connectToAnUnknownRadioEndsDisconnectedRadioNotFound() async {
            let h = await makeSessionHarness()
            let stranger = UUID()
            let events = h.session.events()
            await h.session.connect(radio: stranger)
            let (seen, matched) = await collect(events) { $0.connectionState == .disconnected(reason: "radio not found") }
            #expect(matched)
            #expect(seen.compactMap(\.connectionState) == [.disconnected(reason: nil), .connecting, .disconnected(reason: "radio not found")])
            #expect(!h.log.value.contains("connect"))
        }

        @Test func connectWhilePoweredOffWaitsThenConnects() async {
            let h = await makeSessionHarness(initialState: .poweredOff)
            let events = h.session.events()
            let connecting = Task { await h.session.connect(radio: h.radio.id) }
            let (seenOff, sawUnavailable) = await collect(events) { $0.isAdapterUnavailable }
            #expect(sawUnavailable)
            #expect(seenOff.filter(\.isAdapterUnavailable) == [.adapterUnavailable(.poweredOff)])
            let after = h.session.events()
            CBMCentralManagerMock.simulatePowerOn()
            let (_, configured) = await collect(after, timeout: .seconds(10)) { $0.isConfigured }
            #expect(configured)
            await connecting.value
        }

        @Test func disconnectDropsTheLinkAndReportsDisconnectedWithNilReason() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            await h.session.disconnect()
            let (seen, _) = await collect(h.session.events(), timeout: .seconds(1)) { _ in true }
            #expect(seen.first?.connectionState == .disconnected(reason: nil))
            let stillConnected = await h.radio.central.retrieveConnected([MeshGATT.serviceUUID])
            #expect(!stillConnected.map(\.id).contains(h.radio.id))
        }
    }
}
