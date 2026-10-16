import ComposableArchitecture
@preconcurrency import CoreBluetooth
@preconcurrency import CoreBluetoothMock
import FCBComposable
import Foundation
import MeshProto
import MeshtasticKit
import MeshtasticMock
import Testing

/// Parent suite for ALL tests touching CoreBluetoothMock's process-global
/// simulation. `.serialized` is inherited by nested suites, so no two
/// CBM-backed tests run concurrently. Nest via `extension CBMBackedTests`.
@Suite(.serialized) struct CBMBackedTests {}

/// One simulated Meshtastic radio plus the LIVE FCB client (`mocked()`), which
/// drives it through CoreBluetoothMock — the real FCB actor, not a hand mock.
struct MockRadio {
    let id: UUID
    let device: MockMeshtasticDevice
    let spec: CBMPeripheralSpec
    let central: BluetoothCentralClient
}

/// Resets the CBM simulation and stands up one mock radio (plus `extra` specs).
/// With `initialState: .poweredOn` it waits until the central reports power-on.
@MainActor
func makeMockRadio(
    id: UUID = UUID(),
    modemPreset: Config.LoRaConfig.ModemPreset? = .shortFast,
    initialState: CBMManagerState = .poweredOn,
    extra: [CBMPeripheralSpec] = []
) async -> MockRadio {
    CBMCentralManagerMock.tearDownSimulation()
    CBMCentralManagerMock.simulateInitialState(initialState)
    let device = MockMeshtasticDevice(now: { Date(timeIntervalSince1970: 2_000_000_000) }, modemPreset: modemPreset)
    let spec = device.makeSpec(identifier: id)
    CBMCentralManagerMock.simulatePeripherals([spec] + extra)
    let central = BluetoothCentralClient.mocked()
    if initialState == .poweredOn { await waitUntilPoweredOn(central) }
    return MockRadio(id: id, device: device, spec: spec, central: central)
}

/// Polls FCB's replaying state stream until the mock manager reports power-on.
func waitUntilPoweredOn(_ central: BluetoothCentralClient) async {
    for _ in 0..<200 {
        var iterator = central.state().makeAsyncIterator()
        if await iterator.next() == .poweredOn { return }
        try? await Task.sleep(nanoseconds: 10_000_000)
    }
}

/// Connects + discovers by hand (no MeshtasticSession) so the mock is tested alone.
func rawConnect(_ radio: MockRadio) async throws {
    _ = await radio.central.retrievePeripherals([radio.id])
    _ = try await radio.central.connect(radio.id, ConnectOptions?.none)
    _ = try await radio.central.discoverServices(radio.id, [MeshGATT.serviceUUID])
    _ = try await radio.central.discoverCharacteristics(
        radio.id, MeshGATT.serviceUUID,
        [MeshGATT.toRadioUUID, MeshGATT.fromRadioUUID, MeshGATT.fromNumUUID]
    )
}

func rawWrite(_ toRadio: ToRadio, _ radio: MockRadio) async throws {
    try await radio.central.writeValue(try toRadio.serializedData(), radio.id, MeshGATT.toRadioUUID, .withResponse)
}

/// Reads FromRadio until the radio's queue is drained (empty read). Bounded.
func rawReadAll(_ radio: MockRadio) async throws -> [FromRadio] {
    var frames: [FromRadio] = []
    for _ in 0..<200 {
        let data = try await radio.central.readValue(radio.id, MeshGATT.fromRadioUUID)
        if data.isEmpty { return frames }
        frames.append(try FromRadio(serializedBytes: data))
    }
    Issue.record("FromRadio queue did not drain within 200 reads")
    return frames
}
