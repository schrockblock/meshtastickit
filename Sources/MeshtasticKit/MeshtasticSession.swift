@preconcurrency import CoreBluetooth
import FCBComposable
import Foundation
import MeshProto
import os

/// One Meshtastic radio over BLE: want_config handshake, FromRadio drain, ToRadio
/// send, reconnect. Ported from Meshtastified's MeshSessionActor, made port-agnostic.
/// Connects ONLY via `retrievePeripherals` — it never scans (FCB has one scan per
/// process; scanning belongs to `MeshtasticScanner`).
public actor MeshtasticSession {
    static let maxPayloadLength = 233

    private let central: BluetoothCentralClient
    private let clock: any Clock<Duration>
    private let packetID: @Sendable () -> UInt32
    private let logger = Logger(subsystem: "MeshtasticKit", category: "MeshtasticSession")
    private nonisolated let hub = EventHub()

    /// Bumped by every `connect`/`disconnect`. Attempts, drains and the reconnect
    /// loop re-check it after every await (FCB continuations ignore task
    /// cancellation), so work begun for a superseded link can never emit state or
    /// touch BLE for the current one.
    private var epoch: UInt64 = 0
    private var radioID: UUID?
    private var myNodeNum: UInt32?
    private var modemPreset: Int?
    private var configNonce: UInt32 = 0
    /// Held across the handshake so a FromNum notify mid-handshake cannot start a
    /// steady drain racing it on FCB's single read slot per characteristic.
    private var isDraining = false
    private var drainGeneration: UInt64 = 0
    private var attemptTask: Task<Void, any Error>?
    private var fromNumTask: Task<Void, Never>?

    public init(central: BluetoothCentralClient,
                clock: any Clock<Duration> = ContinuousClock(),
                packetID: @Sendable @escaping () -> UInt32 = { UInt32.random(in: 1 ... .max) }) {
        self.central = central
        self.clock = clock
        self.packetID = packetID
    }

    // MARK: - Events

    /// Multi-subscriber. Registration is synchronous, so a stream created before
    /// `connect` sees every event of that connect. The current connection state
    /// is replayed first; nothing else is buffered for late subscribers.
    public nonisolated func events() -> AsyncStream<MeshtasticEvent> { hub.subscribe() }

    private var connectionState: MeshtasticConnectionState { hub.connection }
    private func setState(_ state: MeshtasticConnectionState) { hub.setConnection(state) }
    private func emit(_ event: MeshtasticEvent) { hub.emit(event) }

    // MARK: - Connect / disconnect

    /// Connects via `retrievePeripherals` — never scans, including on reconnect —
    /// superseding any current link. Always emits `.connecting` first. Returns once
    /// the first attempt resolves: `.connected` + `.configured` emitted, or a
    /// failure state emitted. CoreBluetooth connects do not time out, so for an
    /// out-of-range radio this suspends until it appears or `disconnect()` is called.
    public func connect(radio: UUID) async {
        epoch &+= 1
        let myEpoch = epoch
        await tearDownLink()
        guard myEpoch == epoch else { return }
        radioID = radio
        let outcome = await runAttempt(radio: radio, epoch: myEpoch)
        guard myEpoch == epoch else { return }
        switch outcome {
        case .connected, .superseded:
            return
        case .terminal(let reason), .retryable(let reason):
            radioID = nil
            setState(.disconnected(reason: reason))
        }
    }

    public func disconnect() async {
        epoch &+= 1
        let myEpoch = epoch
        await tearDownLink()
        guard myEpoch == epoch else { return }
        setState(.disconnected(reason: nil))
    }

    /// Cancels the current link's tasks and drops its BLE connection. Callers bump
    /// `epoch` first, so nothing cancelled here can emit afterwards.
    private func tearDownLink() async {
        attemptTask?.cancel()
        attemptTask = nil
        fromNumTask?.cancel()
        fromNumTask = nil
        let old = radioID
        radioID = nil
        if let old { try? await central.disconnect(old) }
    }

    private enum AttemptOutcome {
        case connected, superseded, terminal(String), retryable(String)
    }

    /// A cause retrying cannot fix: the session ends `.disconnected(reason:)`.
    private struct TerminalFailure: Error {
        let reason: String
    }

    /// Runs one attempt in its own task, so `disconnect()` can cancel the parts
    /// that do honor cancellation (the powered-on wait).
    private func runAttempt(radio: UUID, epoch myEpoch: UInt64) async -> AttemptOutcome {
        let task = Task { try await self.attemptConnect(radio: radio, epoch: myEpoch) }
        attemptTask = task
        let result = await task.result
        if attemptTask == task { attemptTask = nil }
        switch result {
        case .success:
            return .connected
        case .failure(let error):
            guard myEpoch == epoch, !(error is CancellationError) else { return .superseded }
            if let terminal = error as? TerminalFailure { return .terminal(terminal.reason) }
            fromNumTask?.cancel()
            fromNumTask = nil
            try? await central.disconnect(radio)
            return myEpoch == epoch ? .retryable(String(describing: error)) : .superseded
        }
    }

    private func check(_ myEpoch: UInt64) throws {
        if myEpoch != epoch || Task.isCancelled { throw CancellationError() }
    }

    private func attemptConnect(radio: UUID, epoch myEpoch: UInt64) async throws {
        try check(myEpoch)
        myNodeNum = nil
        modemPreset = nil
        setState(.connecting)
        try await waitForPoweredOn(epoch: myEpoch)
        // retrievePeripherals (not scan) is the only way this session finds a radio. It also
        // re-stores the live peripheral object in FCB, replacing one CoreBluetooth invalidated
        // in a power cycle (FCB BluetoothCentralClient+Live.swift:271-275).
        let known = await central.retrievePeripherals([radio])
        try check(myEpoch)
        guard known.contains(where: { $0.id == radio }) else {
            throw TerminalFailure(reason: "radio not found")
        }
        _ = try await central.connect(radio, ConnectOptions?.none)
        try check(myEpoch)
        _ = try await central.discoverServices(radio, [MeshGATT.serviceUUID])
        try check(myEpoch)
        _ = try await central.discoverCharacteristics(
            radio, MeshGATT.serviceUUID,
            [MeshGATT.toRadioUUID, MeshGATT.fromRadioUUID, MeshGATT.fromNumUUID]
        )
        try check(myEpoch)
        // Subscribe to FromNum BEFORE want_config so no notify is lost.
        try await central.setNotify(true, radio, MeshGATT.fromNumUUID)
        try check(myEpoch)
        startFromNumLoop(radio: radio, epoch: myEpoch)

        isDraining = true
        drainGeneration &+= 1
        let claimed = drainGeneration
        setState(.configuring)
        do {
            try await requestConfig(radio: radio)
            try await drainUntilConfigComplete(radio: radio, epoch: myEpoch)
            try check(myEpoch)
        } catch {
            if drainGeneration == claimed { isDraining = false }
            throw error
        }
        isDraining = false
        setState(.connected)
        emit(.configured(myNodeNum: myNodeNum ?? 0, modemPreset: modemPreset))
    }

    /// CoreBluetooth silently drops commands issued before `.poweredOn`, and
    /// `retrievePeripherals` returns [] until then — so wait. FCB's state stream
    /// replays the current state, so this returns at once when already on.
    private func waitForPoweredOn(epoch myEpoch: UInt64) async throws {
        var reportedOff = false
        for await state in central.state() {
            try check(myEpoch)
            switch state {
            case .poweredOn:
                return
            case .poweredOff:
                if !reportedOff {
                    reportedOff = true
                    emit(.adapterUnavailable(.poweredOff))
                }
            case .unauthorized:
                emit(.adapterUnavailable(.unauthorized))
                throw TerminalFailure(reason: "unauthorized")
            case .unsupported:
                emit(.adapterUnavailable(.unsupported))
                throw TerminalFailure(reason: "unsupported")
            default:
                continue   // .unknown / .resetting: CoreBluetooth is still settling
            }
        }
        throw CancellationError()   // the stream only ends when this task is cancelled
    }

    private func requestConfig(radio: UUID) async throws {
        var nonce = packetID()
        if nonce == 0 { nonce = 1 }
        configNonce = nonce
        var toRadio = ToRadio()
        toRadio.wantConfigID = nonce
        try await writeToRadio(try toRadio.serializedData(), radio: radio)
    }

    private func writeToRadio(_ bytes: Data, radio: UUID) async throws {
        try await central.writeValue(bytes, radio, MeshGATT.toRadioUUID, .withResponse)
    }

    /// Reads FromRadio until config_complete_id == nonce. Empty reads are a
    /// transient gap during the handshake and are retried; malformed frames skipped.
    private func drainUntilConfigComplete(radio: UUID, epoch myEpoch: UInt64) async throws {
        while true {
            let data = try await central.readValue(radio, MeshGATT.fromRadioUUID)
            try check(myEpoch)
            if data.isEmpty { continue }
            guard let frame = try? FromRadio(serializedBytes: data) else {
                logger.error("skipping malformed FromRadio frame during handshake")
                continue
            }
            process(frame)
            if case .configCompleteID(let completed)? = frame.payloadVariant, completed == configNonce {
                return
            }
        }
    }

    // MARK: - Steady-state drain

    private func startFromNumLoop(radio: UUID, epoch myEpoch: UInt64) {
        fromNumTask?.cancel()
        let notifications = central.notifications(radio, MeshGATT.fromNumUUID)
        fromNumTask = Task { [weak self] in
            do {
                for try await _ in notifications {
                    await self?.drainSteady(radio: radio, epoch: myEpoch)
                }
            } catch {
                await self?.logDrainEnd(error)
            }
        }
    }

    private func logDrainEnd(_ error: any Error) {
        logger.error("FromNum notification stream ended: \(String(describing: error))")
    }

    /// Drains FromRadio until an empty read. A stale drain (older epoch, or one
    /// resuming after a newer handshake claimed the lock) exits without emitting
    /// and without releasing the newer claim.
    private func drainSteady(radio: UUID, epoch myEpoch: UInt64) async {
        guard myEpoch == epoch, !Task.isCancelled, !isDraining else { return }
        isDraining = true
        drainGeneration &+= 1
        let claimed = drainGeneration
        defer { if drainGeneration == claimed { isDraining = false } }
        while myEpoch == epoch, !Task.isCancelled {
            let data: Data
            do {
                data = try await central.readValue(radio, MeshGATT.fromRadioUUID)
            } catch {
                logger.error("FromRadio read failed during drain: \(String(describing: error))")
                return
            }
            guard myEpoch == epoch else { return }
            if data.isEmpty { return }
            guard let frame = try? FromRadio(serializedBytes: data) else {
                logger.error("skipping malformed FromRadio frame during drain")
                continue
            }
            process(frame)
        }
    }

    /// Every decoded frame goes out as `.frame` first; derived events follow.
    private func process(_ frame: FromRadio) {
        emit(.frame(frame))
        switch frame.payloadVariant {
        case .myInfo(let info)?:
            myNodeNum = info.myNodeNum
        case .config(let config)?:
            if case .lora(let lora)? = config.payloadVariant {
                modemPreset = lora.modemPreset.rawValue
            }
        default:
            break
        }
    }
}
