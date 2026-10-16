import ComposableArchitecture
@preconcurrency import CoreBluetooth
@preconcurrency import CoreBluetoothMock
import FCBComposable
import Foundation
import MeshtasticKit
import MeshtasticMock
import Testing

/// CBM only connects a peripheral that has a connection delegate; all defaults succeed.
private struct AcceptingDelegate: CBMPeripheralSpecDelegate {}

private func firstRadio(_ stream: AsyncStream<DiscoveredRadio>, timeout: Duration = .seconds(5)) async -> DiscoveredRadio? {
    await withTaskGroup(of: DiscoveredRadio?.self) { group in
        group.addTask {
            var iterator = stream.makeAsyncIterator()
            return await iterator.next()
        }
        group.addTask {
            try? await Task.sleep(for: timeout)
            return nil
        }
        let first = await group.next() ?? nil
        group.cancelAll()
        return first
    }
}

struct ScannerFakeCentralTests {
    @Test func scanFiltersOnTheMeshtasticServiceUUID() async {
        let captured = LockIsolated<[String]?>(nil)
        let called = LockIsolated(false)
        var central = BluetoothCentralClient.testValue
        central.state = { AsyncStream { $0.yield(.poweredOn) } }
        central.scan = { services, _ in
            captured.setValue(services?.map(\.uuidString))
            called.setValue(true)
            return AsyncStream { _ in }
        }
        let consumer = Task { for await _ in MeshtasticScanner.scan(central: central) {} }
        let scanned = await waitUntil { called.value }
        consumer.cancel()
        #expect(scanned)
        #expect(captured.value == [MeshGATT.serviceUUID.uuidString])
    }

    @Test func cancellingTheConsumerEndsTheUnderlyingScan() async {
        let started = LockIsolated(false)
        let terminated = LockIsolated(false)
        var central = BluetoothCentralClient.testValue
        central.state = { AsyncStream { $0.yield(.poweredOn) } }
        central.scan = { _, _ in
            started.setValue(true)
            return AsyncStream { continuation in
                continuation.onTermination = { _ in terminated.setValue(true) }
            }
        }
        let consumer = Task { for await _ in MeshtasticScanner.scan(central: central) {} }
        let didStart = await waitUntil { started.value }
        #expect(didStart)
        consumer.cancel()
        let ended = await waitUntil(timeout: .seconds(2)) { terminated.value }
        #expect(ended)
    }
}

extension CBMBackedTests {
    @Suite @MainActor
    struct ScannerTests {
        /// Live FCB client against CBM: a scan issued while powered off must wait —
        /// CoreBluetooth silently drops a scan issued before .poweredOn.
        @Test func scanWaitsForPoweredOnThenYieldsTheMockRadio() async {
            let radio = await makeMockRadio(initialState: .poweredOff)
            let stream = MeshtasticScanner.scan(central: radio.central)
            let found = await withTaskGroup(of: DiscoveredRadio?.self) { group in
                group.addTask { await firstRadio(stream, timeout: .seconds(5)) }
                group.addTask {
                    try? await Task.sleep(for: .milliseconds(50))
                    CBMCentralManagerMock.simulatePowerOn()
                    return nil
                }
                var result: DiscoveredRadio?
                for await value in group where value != nil { result = value }
                return result
            }
            #expect(found?.id == radio.id)
            #expect(found?.name == "Meshtastic_MOCK")
        }

        /// A radio that advertises only the service UUID (no local name) is named
        /// from its peripheral name. Live FCB client against CBM.
        @Test func nameFallsBackToThePeripheralNameWhenNotAdvertised() async {
            let id = UUID()
            CBMCentralManagerMock.tearDownSimulation()
            CBMCentralManagerMock.simulateInitialState(.poweredOn)
            let spec = CBMPeripheralSpec
                .simulatePeripheral(identifier: id, proximity: .near)
                .advertising(advertisementData: [
                    CBMAdvertisementDataServiceUUIDsKey: [CBMUUID(string: MeshGATT.serviceUUID.uuidString)],
                    CBMAdvertisementDataIsConnectable: true as NSNumber,
                ], withInterval: 0.05)
                .connectable(name: "PeripheralName", services: [], delegate: AcceptingDelegate(), connectionInterval: 0.01)
                .allowForRetrieval()
                .build()
            CBMCentralManagerMock.simulatePeripherals([spec])
            let central = BluetoothCentralClient.mocked()
            await waitUntilPoweredOn(central)
            // CBM (like iOS) only knows the GATT device name once the peripheral
            // has been connected; before that `name` is just the advertised name.
            _ = await central.retrievePeripherals([id])
            _ = try? await central.connect(id, ConnectOptions?.none)
            try? await central.disconnect(id)
            let radio = await firstRadio(MeshtasticScanner.scan(central: central))
            #expect(radio?.id == id)
            #expect(radio?.name == "PeripheralName")
        }
    }
}
