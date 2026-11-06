# MeshtasticKit

A Swift package for talking to [Meshtastic](https://meshtastic.org) LoRa radios over Bluetooth Low Energy from iOS, iPadOS, Mac Catalyst and macOS apps.

It connects to one radio, runs the `want_config` handshake, reads every `FromRadio` frame the radio queues, sends `ToRadio` data packets on any port, and reconnects with backoff when the link drops. It hands you decoded protobufs and a few derived events. It does not keep a node database, manage channels or configure the radio.

MeshtasticKit was written as the radio layer for ReTiCA, a Swift implementation of [Reticulum](https://reticulum.network), which uses it to carry Reticulum packets over a Meshtastic mesh. Nothing in it is specific to Reticulum.

## Status

Early and pre-1.0. There are no tagged releases yet. The public API may change.

Everything below has been tested against a simulated radio (see [Testing](#testing)). Behaviour against real hardware has not been verified in this repository. See [Limitations](#limitations-and-unverified-areas).

## What is supported

| Area | Supported |
| --- | --- |
| Transport | Bluetooth LE only (the Meshtastic GATT service). No USB serial, no TCP/Wi-Fi, no MQTT. |
| Platforms | iOS 17+, Mac Catalyst 17+, macOS 13+ |
| Toolchain | Swift 6.3 tools (`swift-tools-version: 6.3`), Swift 6 language mode |
| Protobufs | Generated from [meshtastic/protobufs](https://github.com/meshtastic/protobufs) at commit `da2fc413931c81c1d2bc9b6a838d739e2bf35bc1` (13 July 2026) |
| Firmware | Not pinned. The package uses the BLE GATT layout and protobuf fields current at that protobuf commit. It has not been tested against specific firmware releases. |

### Protocol coverage

What `MeshtasticSession` actually does on the wire:

- **GATT**: service `6ba1b218-15a8-461f-9fa8-5dcae273eafd`, writing `ToRadio` (`f75c76d2-…`), reading `FromRadio` (`2c55e69e-…`), and subscribing to `FromNum` (`ed9da18c-…`) notifications. The UUIDs are public as `MeshGATT`.
- **Handshake**: subscribes to `FromNum`, writes `ToRadio.want_config_id` with a random nonce, then reads `FromRadio` until `config_complete_id` matches. It times out after 30 seconds.
- **Receive**: each `FromNum` notification starts a drain, which reads `FromRadio` until an empty read. The session also drains once after the handshake, which picks up packets the radio queued while no phone was connected.
- **Send**: one `MeshPacket` holding a decoded `Data` message on any port number, with `to`, `channel`, `want_ack` and an optional `hop_limit`. The payload limit is 233 bytes (`DATA_PAYLOAD_LEN`). Writes are serialised, one at a time, with response.
- **Routing**: a `ROUTING_APP` packet is decoded and reported as `.routing(packetID:ok:)`, where `ok` means `error_reason == NONE`.
- **From the config stream**: `MyNodeInfo.my_node_num`, and the LoRa `modem_preset` if the radio sends a LoRa config.

What it does not do:

- No admin messages: it cannot set config, channels, owner or region, and cannot reboot the radio.
- No `heartbeat`, no use of `QueueStatus` for back-pressure, no XModem file transfer, no firmware update.
- No node database, position, telemetry or text-message model of its own. Every decoded `FromRadio` frame is passed through as `.frame(FromRadio)`, so you can build these from the `MeshProto` types.
- No PKI or channel crypto in the client. The radio decrypts. Packets that reach the phone still encrypted (no `decoded` payload) are not reported as `.packet`, though they still appear inside `.frame`.

## Products

| Product | What it is |
| --- | --- |
| `MeshtasticKit` | `MeshtasticScanner`, `MeshtasticSession`, the event and value types, and `MeshGATT`. |
| `MeshProto` | The Meshtastic protobufs as public Swift types (`FromRadio`, `ToRadio`, `MeshPacket`, `PortNum`, `Config`, `NodeInfo`, ...), generated with SwiftProtobuf. |
| `MeshtasticMock` | `MockMeshtasticDevice`, a scripted Meshtastic radio built on Nordic's [CoreBluetoothMock](https://github.com/NordicSemiconductor/IOS-CoreBluetooth-Mock), for tests and the iOS Simulator. |

Dependencies:

- [FCB](https://github.com/schrockblock/fcb) (`FCBComposable`): an async wrapper around CoreBluetooth. MeshtasticKit does all of its Bluetooth work through FCB's `BluetoothCentralClient`.
- [swift-protobuf](https://github.com/apple/swift-protobuf) 1.28.0 or later.
- [IOS-CoreBluetooth-Mock](https://github.com/NordicSemiconductor/IOS-CoreBluetooth-Mock) 1.0.0 or later, used by `MeshtasticMock` and the tests. FCB also uses it, so it is linked into apps either way.
- [swift-composable-architecture](https://github.com/pointfreeco/swift-composable-architecture) 1.0.0 or later. The tests use it directly, and FCB already depends on it.

## Installation

**FCB is a local path dependency.** `Package.swift` declares `.package(path: "../FCB")`, so MeshtasticKit only resolves when FCB is checked out in a sibling directory named `FCB`. SwiftPM does not allow a package fetched by URL to depend on a local path. Until that line changes to a URL dependency, you cannot add MeshtasticKit to a project by its GitHub URL. Instead:

1. Clone both repositories side by side:

   ```
   some-folder/
     FCB/            git clone https://github.com/schrockblock/fcb FCB
     MeshtasticKit/  git clone https://github.com/schrockblock/meshtastickit MeshtasticKit
   ```

2. Add MeshtasticKit to your package by path. Also add FCB if you need `BluetoothCentralClient` directly, which you do to create a session:

   ```swift
   dependencies: [
       .package(path: "../some-folder/MeshtasticKit"),
       .package(path: "../some-folder/FCB"),
   ],
   targets: [
       .target(name: "MyApp", dependencies: [
           .product(name: "MeshtasticKit", package: "MeshtasticKit"),
           .product(name: "MeshProto", package: "MeshtasticKit"),
           .product(name: "FCBComposable", package: "FCB"),
       ]),
   ]
   ```

   In an Xcode project, use File > Add Package Dependencies > Add Local, and select the `MeshtasticKit` folder.

## Usage

```swift
import FCBComposable
import Foundation
import MeshProto
import MeshtasticKit

func findAndTalk() async throws {
    let central = BluetoothCentralClient.liveValue

    // 1. Scan for radios advertising the Meshtastic service. Stop iterating to end the scan.
    var radioID: UUID?
    for await radio in MeshtasticScanner.scan(central: central) {
        print("found \(radio.name ?? "unnamed") rssi \(radio.rssi)")
        radioID = radio.id
        break
    }
    guard let radioID else { return }

    // 2. Subscribe before connecting so no event of this connect is missed.
    let session = MeshtasticSession(central: central)
    let events = session.events()
    Task {
        for await event in events {
            switch event {
            case .connection(let state):
                print("state: \(state)")
            case .configured(let myNodeNum, let modemPreset):
                print("configured as !\(String(myNodeNum, radix: 16)), preset \(String(describing: modemPreset))")
            case .packet(let from, _, _, let portnum, let payload, _)
                where portnum == UInt32(PortNum.textMessageApp.rawValue):
                print("text from !\(String(from, radix: 16)): \(String(decoding: payload, as: UTF8.self))")
            case .routing(let packetID, let ok):
                print("packet \(packetID) \(ok ? "acknowledged" : "failed")")
            default:
                break
            }
        }
    }

    // 3. Connect (returns once the want_config handshake ends or the first attempt fails).
    await session.connect(radio: radioID)

    // 4. Send a broadcast text on the primary channel.
    let id = try await session.send(MeshtasticOutbound(
        portnum: UInt32(PortNum.textMessageApp.rawValue),
        payload: Data("hello mesh".utf8),
        wantAck: true
    ))
    print("sent packet \(id)")

    await session.disconnect()
}
```

This example compiles against the current API. It has not been run against a real radio.

### The API in brief

- `MeshtasticScanner.scan(central:) -> AsyncStream<DiscoveredRadio>` is the only scanning entry point. It waits for Bluetooth to power on, filters on the Meshtastic service, and stops its scan when you stop iterating. FCB runs one scan per central, so a newer scan by anything else in the process ends this stream. If Bluetooth is power-cycled, the scanner scans again.
- `MeshtasticSession(central:clock:packetID:)` is an actor for one radio at a time. It never scans. `connect(radio:)` finds the radio with `retrievePeripherals`, so the UUID must be one CoreBluetooth already knows on this device, typically from a scan. A new `connect` replaces the current link.
- `events()` returns an `AsyncStream<MeshtasticEvent>`. It accepts any number of subscribers and replays the current connection state first. Nothing else is buffered for late subscribers.
- `MeshtasticEvent` cases:
  - `.connection(MeshtasticConnectionState)` reports `disconnected(reason:)`, `connecting`, `configuring`, `connected` and `reconnecting(attempt:)`.
  - `.configured(myNodeNum:modemPreset:)` arrives after every completed handshake, including after each reconnect. `modemPreset` is the raw `Config.LoRaConfig.ModemPreset` value.
  - `.packet(from:to:channel:portnum:payload:id:)` covers every decoded `MeshPacket` on any port.
  - `.routing(packetID:ok:)` is the routing result for a packet you sent.
  - `.frame(FromRadio)` passes every decoded frame through unfiltered. Each `MeshPacket` arrives both as `.frame` and as `.packet`, so handle one or the other, not both.
  - `.adapterUnavailable(...)` reports `poweredOff`, `unauthorized` or `unsupported`. `unauthorized` and `unsupported` are terminal.
- `send(_:)` throws `MeshtasticSessionError.payloadTooLarge` (over 233 bytes) or `.notConnected`. It returns the packet id, which you can match against `.routing`.
- **Reconnect**: after a lost link, a failed attempt, a Bluetooth power-off or a handshake timeout, the session retries with backoff of 1, 2, 4, 8 and 16 seconds, then every 30 seconds. It stops retrying on a terminal cause ("radio not found", "unauthorized", "unsupported"), on `disconnect()`, or on a new `connect`.
- **Sharing Bluetooth**: FCB's central is process-wide. Use the same `BluetoothCentralClient` (normally `.liveValue`) for the scanner, the session and any other Bluetooth code in the app.

## Testing

```
swift test
```

Run it from the package directory, with FCB checked out beside it. The tests run on macOS against CoreBluetoothMock rather than hand-written stubs: FCB's real client is created with `BluetoothCentralClient.mocked()` and talks to a `MockMeshtasticDevice`. They cover the handshake, receiving, sending and the write queue, reconnect and backoff on a test clock, power-off during each phase, and the scanner.

You can use the same setup in your own tests or in a Simulator build:

```swift
import CoreBluetoothMock
import FCBComposable
import MeshtasticKit
import MeshtasticMock

let radioID = UUID()
let device = MockMeshtasticDevice()     // canned node DB, LongFast preset, ACKs every packet
CBMCentralManagerMock.simulateInitialState(.poweredOn)
CBMCentralManagerMock.simulatePeripherals([device.makeSpec(identifier: radioID)])

let session = MeshtasticSession(central: BluetoothCentralClient.mocked())
await session.connect(radio: radioID)

device.injectIncomingText("hi", from: MockMeshtasticDevice.nodeNums[0])
device.ackBehavior = .nak(.noRoute)     // or .ack, .silent
print(device.receivedPackets)           // every MeshPacket written to the radio
```

`MockMeshtasticDevice` also provides:

- `injectIncomingPacket(portnum:payload:from:...)`, `injectFrame(_:)` and `injectRawFrame(_:notify:)` to feed traffic to the client.
- `answersWantConfig = false` to test the handshake timeout.
- `onPacketWrite` and `onFromRadioRead` hooks to act while a write or read is still pending.
- `startPeriodicTraffic(interval:)` for demo traffic in the Simulator.

When a text DM is sent to one of its fake nodes, the mock replies about 3 seconds later. CoreBluetoothMock's simulation is process-global, so tests that use it must not run in parallel. The test suite nests them under one `.serialized` suite.

## Permissions and entitlements

MeshtasticKit uses CoreBluetooth as a central. Your app needs:

- **iOS, iPadOS and Mac Catalyst**: `NSBluetoothAlwaysUsageDescription` in `Info.plist`. Without it, the app is terminated on first Bluetooth use. If the user denies access, the session ends in `.disconnected(reason: "unauthorized")`.
- **macOS (sandboxed)**: the `com.apple.security.device.bluetooth` entitlement, plus `NSBluetoothAlwaysUsageDescription`.
- **Pairing**: the radio's `FromNum` characteristic requires encryption, so the first subscription triggers system pairing. Depending on the radio's Bluetooth settings, the radio shows or uses a PIN, which the user enters in the system prompt.
- **Background**: the package does not opt into CoreBluetooth state restoration. Keeping a link alive in the background needs the `bluetooth-central` background mode in your app, and that has not been verified with this package.

## Regenerating the protobufs

`scripts/generate-protos.sh` clones `meshtastic/protobufs` at the pinned commit and regenerates `Sources/MeshProto/Generated` with `protoc` and `protoc-gen-swift`, with public visibility. It compiles `mesh.proto` and its imports: `channel`, `config`, `device_ui`, `module_config`, `portnums`, `telemetry`, `xmodem` and `atak`. The script's output path is currently an absolute path on the author's machine. Edit `OUT` before running it.

## Limitations and unverified areas

- **No real-hardware verification.** The connect, handshake, send, receive, reconnect and power-off behaviour has been verified only against CoreBluetoothMock. A hardware test checklist exists in the ReTiCA project but has not been run. The following are all unconfirmed: firmware compatibility, pairing flow, the 30 s handshake timeout against a busy node database, and behaviour with a radio out of range.
- **BLE only.** No serial or TCP transport.
- **One radio per session.** Running several sessions to different radios at once has not been tested.
- **Connect does not time out while the radio is out of range.** CoreBluetooth connects never time out, so `connect(radio:)` suspends until the radio appears or you call `disconnect()`. The 30 s timeout applies only to the handshake after connection.
- **`connect` needs a known peripheral.** If CoreBluetooth cannot retrieve the UUID on this device, which can happen on a new install or a different device, the session ends with "radio not found". Scan first.
- **The scanner waits indefinitely when Bluetooth is unauthorized or unsupported.** Watch the adapter state yourself if you need to give up.
- **Send back-pressure**: nothing reads `QueueStatus`, and firmware duty-cycle limits (for example EU 868 MHz) can refuse a send. A refused send shows up only as a failed or missing routing result.
- **Not CocoaPods or Carthage.** SwiftPM only.

## Credits and licences

The `MeshProto` sources are generated from the [Meshtastic protobufs](https://github.com/meshtastic/protobufs) by the Meshtastic project. The protobuf definitions are licensed under the GNU General Public License v3.0 in that repository. No licence file for them is included here. Read the upstream licence before redistributing code built on `MeshProto`.

The BLE session logic was adapted from the author's earlier Meshtastic client code. The Bluetooth layer is [FCB](https://github.com/schrockblock/fcb), and the mock radio runs on Nordic Semiconductor's [CoreBluetoothMock](https://github.com/NordicSemiconductor/IOS-CoreBluetooth-Mock).

This repository does not yet include a licence for MeshtasticKit itself.

Meshtastic is a registered trademark of Meshtastic LLC. This project is not affiliated with or endorsed by the Meshtastic project.
