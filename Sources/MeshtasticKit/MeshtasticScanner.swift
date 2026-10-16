@preconcurrency import CoreBluetooth
import FCBComposable
import Foundation

public enum MeshtasticScanner {
    /// The only scanning entry point. Supersedes any other FCB scan in the process while iterated.
    /// Waits for `.poweredOn` (CoreBluetooth silently drops an earlier scan), filters on the
    /// Meshtastic service UUID, and ends the FCB scan when the consumer stops iterating.
    public static func scan(central: BluetoothCentralClient) -> AsyncStream<DiscoveredRadio> {
        AsyncStream { continuation in
            let task = Task {
                for await state in central.state() {
                    if Task.isCancelled { continuation.finish(); return }
                    if state == .poweredOn { break }
                }
                if Task.isCancelled { continuation.finish(); return }
                for await found in central.scan([MeshGATT.serviceUUID], ScanOptions?.none) {
                    continuation.yield(DiscoveredRadio(
                        id: found.peripheral.id,
                        name: found.advertisementData.localName ?? found.peripheral.name,
                        rssi: found.rssi
                    ))
                }
                continuation.finish()
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}
