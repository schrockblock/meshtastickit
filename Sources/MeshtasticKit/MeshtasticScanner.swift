@preconcurrency import CoreBluetooth
import FCBComposable
import Foundation

public enum MeshtasticScanner {
    /// The only scanning entry point. Waits for `.poweredOn` (CoreBluetooth silently drops an
    /// earlier scan), filters on the Meshtastic service UUID, and ends ITS OWN FCB scan when the
    /// consumer stops iterating (stream termination is FCB's only stop; it never calls `stopScan`).
    ///
    /// FCB runs one radio scan per central. When FCB ends this scan, the scanner looks at the
    /// adapter: Bluetooth left `.poweredOn` → wait for power and scan again; still `.poweredOn` →
    /// a newer scan by another owner superseded this one, so the stream finishes for good.
    public static func scan(central: BluetoothCentralClient) -> AsyncStream<DiscoveredRadio> {
        AsyncStream { continuation in
            let task = Task {
                while await waitForPoweredOn(central) {
                    for await found in central.scan([MeshGATT.serviceUUID], ScanOptions?.none) {
                        continuation.yield(DiscoveredRadio(
                            id: found.peripheral.id,
                            name: found.advertisementData.localName ?? found.peripheral.name,
                            rssi: found.rssi
                        ))
                    }
                    if Task.isCancelled { break }
                    // Superseded while Bluetooth stayed on: another owner has the radio now.
                    if await currentState(central) == .poweredOn { break }
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }

    /// `true` once the adapter reports `.poweredOn`; `false` if cancelled first or the state
    /// stream ended.
    private static func waitForPoweredOn(_ central: BluetoothCentralClient) async -> Bool {
        for await state in central.state() {
            if Task.isCancelled { return false }
            if state == .poweredOn { return true }
        }
        return false
    }

    /// The adapter's current state (FCB's state stream replays it first).
    private static func currentState(_ central: BluetoothCentralClient) async -> CBManagerState? {
        for await state in central.state() { return state }
        return nil
    }
}
