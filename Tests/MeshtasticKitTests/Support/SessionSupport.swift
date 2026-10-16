import ComposableArchitecture
@preconcurrency import CoreBluetooth
@preconcurrency import CoreBluetoothMock
import FCBComposable
import Foundation
import MeshProto
import MeshtasticKit
import MeshtasticMock
import Testing

// MARK: Event predicates (MeshtasticEvent is Equatable — assert with `==`; these only
// pick events out of a stream)

extension MeshtasticEvent {
    var connectionState: MeshtasticConnectionState? {
        if case .connection(let state) = self { return state }
        return nil
    }
    var isConfigured: Bool {
        if case .configured = self { return true }
        return false
    }
    /// The id of a `.packet` event (nil for every other case).
    var packetID: UInt32? {
        if case .packet(_, _, _, _, _, let id) = self { return id }
        return nil
    }
    var isRouting: Bool {
        if case .routing = self { return true }
        return false
    }
    var frameValue: FromRadio? {
        if case .frame(let frame) = self { return frame }
        return nil
    }
    var isAdapterUnavailable: Bool {
        if case .adapterUnavailable = self { return true }
        return false
    }
}

// MARK: Collection

/// Iterates `stream` until `stop` matches (inclusive) or `timeout` (real time)
/// passes. Returns everything seen and whether `stop` matched. CONSUMES the
/// stream: a later collect needs a fresh `session.events()` created BEFORE the
/// action whose events it should see.
func collect(_ stream: AsyncStream<MeshtasticEvent>, timeout: Duration = .seconds(5),
             until stop: @escaping @Sendable (MeshtasticEvent) -> Bool) async -> (events: [MeshtasticEvent], matched: Bool) {
    let box = LockIsolated<[MeshtasticEvent]>([])
    let matched = await withTaskGroup(of: Bool.self) { group in
        group.addTask {
            for await event in stream {
                box.withValue { $0.append(event) }
                if stop(event) { return true }
            }
            return false
        }
        group.addTask {
            try? await Task.sleep(for: timeout)
            return false
        }
        let first = await group.next() ?? false
        group.cancelAll()
        return first
    }
    return (box.value, matched)
}

/// Polls `condition` every 10 ms until true or `timeout` (real time).
func waitUntil(timeout: Duration = .seconds(5), _ condition: @Sendable () -> Bool) async -> Bool {
    let clock = ContinuousClock()
    let deadline = clock.now.advanced(by: timeout)
    while clock.now < deadline {
        if condition() { return true }
        try? await Task.sleep(for: .milliseconds(10))
    }
    return condition()
}

/// Sequential packet ids starting at `start`.
func makeCounter(start: UInt32 = 0x100) -> @Sendable () -> UInt32 {
    let next = LockIsolated(start)
    return { next.withValue { value in defer { value &+= 1 }; return value } }
}

// MARK: Centrals

/// Wraps a LIVE central, logging "scan" / "retrievePeripherals" / "connect" calls
/// in order; every call still reaches the real FCB actor.
func recording(_ base: BluetoothCentralClient, into log: LockIsolated<[String]>) -> BluetoothCentralClient {
    var client = base
    client.scan = { services, options in
        log.withValue { $0.append("scan") }
        return base.scan(services, options)
    }
    client.retrievePeripherals = { ids in
        log.withValue { $0.append("retrievePeripherals") }
        return await base.retrievePeripherals(ids)
    }
    client.connect = { id, options in
        log.withValue { $0.append("connect") }
        return try await base.connect(id, options)
    }
    return client
}

/// A closure-backed central for paths CoreBluetoothMock cannot drive
/// deterministically (adapter authorization, failing or gated connects, backoff
/// timing). Every endpoint the session can touch is set. It NEVER completes a
/// handshake (reads are empty), so use it only for paths that end before
/// `.configuring` completes.
func fakeCentral(radio: UUID, state: CBManagerState = .poweredOn, log: LockIsolated<[String]>,
                 connect: @escaping @Sendable () async throws -> Void) -> BluetoothCentralClient {
    BluetoothCentralClient(
        state: { AsyncStream { $0.yield(state) } },
        authorization: { .allowedAlways },
        scan: { _, _ in
            log.withValue { $0.append("scan") }
            return .never
        },
        stopScan: {},
        connect: { id, _ in
            log.withValue { $0.append("connect") }
            try await connect()
            return Peripheral(id: id, name: "Fake", state: .connected)
        },
        disconnect: { _ in log.withValue { $0.append("disconnect") } },
        retrievePeripherals: { ids in
            log.withValue { $0.append("retrievePeripherals") }
            return ids.filter { $0 == radio }.map { Peripheral(id: $0, name: "Fake", state: .disconnected) }
        },
        retrieveConnected: { _ in [] },
        discoverServices: { _, _ in [] },
        discoverCharacteristics: { _, _, _ in [] },
        readValue: { _, _ in Data() },
        writeValue: { _, _, _, _ in },
        setNotify: { _, _, _ in },
        maximumWriteValueLength: { _, _ in 512 },
        notifications: { _, _ in AsyncThrowingStream { _ in } },
        openL2CAPChannel: { _, _ in throw BluetoothError.l2capNotSupported },
        events: { AsyncStream { _ in } },
        canSendWriteWithoutResponse: { _ in true }
    )
}

// MARK: Harness

/// A mock radio + a MeshtasticSession over the LIVE FCB mocked client, with call logging.
struct SessionHarness {
    let radio: MockRadio
    let session: MeshtasticSession
    let log: LockIsolated<[String]>
}

/// Default clock is the REAL `ContinuousClock`, never `ImmediateClock`: the session's
/// 30 s want_config timeout sleeps on the injected clock, and an ImmediateClock would
/// fire it at once and kill every handshake. Backoff then costs 1 s of real time in
/// reconnect tests; pass a `TestClock` to step time explicitly.
@MainActor
func makeSessionHarness(
    id: UUID = UUID(),
    clock: any Clock<Duration> = ContinuousClock(),
    modemPreset: Config.LoRaConfig.ModemPreset? = .shortFast,
    initialState: CBMManagerState = .poweredOn,
    extra: [CBMPeripheralSpec] = [],
    packetID: @escaping @Sendable () -> UInt32 = makeCounter()
) async -> SessionHarness {
    let radio = await makeMockRadio(id: id, modemPreset: modemPreset, initialState: initialState, extra: extra)
    let log = LockIsolated<[String]>([])
    let session = MeshtasticSession(central: recording(radio.central, into: log), clock: clock, packetID: packetID)
    return SessionHarness(radio: radio, session: session, log: log)
}

// MARK: Clocks and gates

/// Records every requested sleep and returns at once. `now` never advances, so a
/// `sleep(for: d)` arrives as a deadline offset of exactly `d`.
struct SleepRecordingClock: Clock {
    struct Instant: InstantProtocol {
        var offset: Duration
        func advanced(by duration: Duration) -> Instant { Instant(offset: offset + duration) }
        func duration(to other: Instant) -> Duration { other.offset - offset }
        static func < (lhs: Instant, rhs: Instant) -> Bool { lhs.offset < rhs.offset }
    }

    let sleeps = LockIsolated<[Duration]>([])
    var now: Instant { Instant(offset: .zero) }
    var minimumResolution: Duration { .zero }

    func sleep(until deadline: Instant, tolerance: Duration?) async throws {
        try Task.checkCancellation()
        sleeps.withValue { $0.append(deadline.offset) }
        await Task.yield()
    }
}

/// Suspends `wait()` callers until `open()`.
actor AsyncGate {
    private var isOpen = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        if isOpen { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    func open() {
        isOpen = true
        for waiter in waiters { waiter.resume() }
        waiters = []
    }
}
