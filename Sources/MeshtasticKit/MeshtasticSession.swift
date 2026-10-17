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
    /// A FromNum notify arrived while the drain lock was held.
    private var drainRequested = false
    private var attemptTask: Task<Void, any Error>?
    /// The attempt `runAttempt` is waiting on. Its outcome is SESSION-owned: the
    /// attempt task, the handshake timeout, the adapter leaving `.poweredOn`, or
    /// teardown — whichever comes first — resolves it. FCB once failed no pending op
    /// on a power-off (fixed in FCB, Meshtastic lane F); an attempt still never depends
    /// on FCB to end. An attempt that
    /// lost the race is abandoned: its id no longer matches, so every `check` in it
    /// throws and it can never emit state or touch BLE again.
    private var attemptSeq: UInt64 = 0
    private var currentAttempt: UInt64 = 0
    /// The last attempt whose `waitForPoweredOn` returned (see `adapterLeftPoweredOn`).
    private var attemptPassedPowerGate: UInt64 = 0
    private var attemptOutcome: CheckedContinuation<Result<Void, any Error>, Never>?
    private var fromNumTask: Task<Void, Never>?
    private var disconnectWatchTask: Task<Void, Never>?
    private var reconnectTask: Task<Void, Never>?
    static let maxBackoff: Duration = .seconds(30)
    static let handshakeTimeout: Duration = .seconds(30)
    /// FIFO slot for ToRadio writes: FCB holds one pending write per characteristic,
    /// and a second write fails the first with `operationCancelled`.
    private var writeInFlight = false
    /// Each waiter is resumed with the generation the slot was handed over in.
    private var writeWaiters: [CheckedContinuation<UInt64, any Error>] = []
    /// Bumped by `resetWriteSlot()` on every link teardown. Older FCB leaked a pending
    /// write forever at a power-off; the slot still never waits on FCB, so a writer that never
    /// returns must not hold the slot for the next link; a stale writer that does
    /// return releases nothing.
    private var writeGeneration: UInt64 = 0
    /// The write the slot holder is waiting on. Its completion is SESSION-owned:
    /// FCB's result or teardown (`resetWriteSlot()` throws `notConnected`),
    /// whichever comes first. A losing FCB write is abandoned: its late result
    /// carries an old `writeSeq` and is ignored.
    private var writeSeq: UInt64 = 0
    private var inFlightWrite: (seq: UInt64, completion: CheckedContinuation<Void, any Error>)?

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
        // Before any await: even a connect superseded at once has emitted it.
        setState(.connecting)
        await tearDownLink()
        guard myEpoch == epoch else { return }
        radioID = radio
        let outcome = await runAttempt(radio: radio, epoch: myEpoch)
        guard myEpoch == epoch else { return }
        switch outcome {
        case .connected, .superseded:
            return
        case .terminal(let reason):
            radioID = nil
            setState(.disconnected(reason: reason))
        case .retryable(let reason):
            logger.error("connect attempt failed: \(reason)")
            scheduleReconnect(radio: radio, epoch: myEpoch)
        }
    }

    public func disconnect() async {
        epoch &+= 1
        let myEpoch = epoch
        await tearDownLink()
        guard myEpoch == epoch else { return }
        setState(.disconnected(reason: nil))
    }

    // MARK: - Send

    /// Writes one DataMessage on `p.portnum`. Returns the packet id whose routing
    /// result (if the radio sends one) arrives later as `.routing`.
    public func send(_ p: MeshtasticOutbound) async throws -> UInt32 {
        guard p.payload.count <= Self.maxPayloadLength else { throw MeshtasticSessionError.payloadTooLarge }
        guard connectionState == .connected, let radio = radioID else { throw MeshtasticSessionError.notConnected }
        var id = packetID()
        if id == 0 { id = 1 }
        var data = DataMessage()
        data.portnum = PortNum(rawValue: Int(p.portnum)) ?? .unknownApp
        data.payload = p.payload
        var packet = MeshPacket()
        packet.to = p.to
        packet.channel = p.channel
        packet.id = id
        packet.wantAck = p.wantAck
        if let hopLimit = p.hopLimit { packet.hopLimit = hopLimit }
        packet.decoded = data
        var toRadio = ToRadio()
        toRadio.packet = packet
        try await writeToRadio(try toRadio.serializedData(), radio: radio)
        return id
    }

    /// Cancels the current link's tasks and drops its BLE connection. Callers bump
    /// `epoch` first, so nothing cancelled here can emit afterwards.
    private func tearDownLink() async {
        reconnectTask?.cancel()
        reconnectTask = nil
        disconnectWatchTask?.cancel()
        disconnectWatchTask = nil
        // A stuck attempt (an FCB op that never resolves) would otherwise keep
        // connect() suspended forever: resolve it as superseded.
        abortAttempt(nil, CancellationError())
        attemptTask?.cancel()
        attemptTask = nil
        fromNumTask?.cancel()
        fromNumTask = nil
        resetWriteSlot()
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

    /// The handshake got no config_complete within `handshakeTimeout`.
    private struct HandshakeTimedOut: Error {}
    /// The adapter left `.poweredOn` mid-attempt (iOS reports a power-off as a
    /// state change only; the session ends the attempt itself instead of waiting on FCB).
    private struct AdapterLeftPoweredOn: Error {}

    /// Runs one attempt in its own task, so `disconnect()` can cancel the parts
    /// that do honor cancellation (the powered-on wait), and waits on the
    /// session-owned outcome rather than on the task itself.
    private func runAttempt(radio: UUID, epoch myEpoch: UInt64) async -> AttemptOutcome {
        attemptSeq &+= 1
        let id = attemptSeq
        currentAttempt = id
        let task = Task { try await self.attemptConnect(radio: radio, epoch: myEpoch, attempt: id) }
        attemptTask = task
        let powerWatch = startAttemptPowerWatch(attempt: id)
        let result: Result<Void, any Error> = await withCheckedContinuation { outcome in
            attemptOutcome = outcome
            Task {   // actor-isolated: resolves on the session
                let finished = await task.result
                self.resolveAttempt(id, finished)
            }
        }
        powerWatch.cancel()
        if attemptTask == task { attemptTask = nil }
        switch result {
        case .success:
            return .connected
        case .failure(let error):
            guard myEpoch == epoch, !(error is CancellationError) else { return .superseded }
            if let terminal = error as? TerminalFailure { return .terminal(terminal.reason) }
            fromNumTask?.cancel()
            fromNumTask = nil
            resetWriteSlot()
            try? await central.disconnect(radio)
            return myEpoch == epoch ? .retryable(String(describing: error)) : .superseded
        }
    }

    /// The attempt task finished (either way). Ignored if the attempt was already
    /// resolved by a timeout, a power-off or teardown.
    private func resolveAttempt(_ id: UInt64, _ result: Result<Void, any Error>) {
        guard id == currentAttempt, let outcome = attemptOutcome else { return }
        attemptOutcome = nil
        outcome.resume(returning: result)
    }

    /// Fails attempt `id` (any current attempt when nil) now, abandoning whatever
    /// FCB call it is suspended in.
    private func abortAttempt(_ id: UInt64?, _ error: any Error) {
        guard let outcome = attemptOutcome, id == nil || id == currentAttempt else { return }
        attemptOutcome = nil
        currentAttempt = 0
        attemptTask?.cancel()
        outcome.resume(returning: .failure(error))
    }

    /// Watches the adapter for the whole attempt: once it has been `.poweredOn`,
    /// leaving that state while still `.connecting`/`.configuring` fails the
    /// attempt (retryably). After `.configured` the disconnect watch takes over.
    private func startAttemptPowerWatch(attempt id: UInt64) -> Task<Void, Never> {
        let states = central.state()
        return Task { [weak self] in
            var sawPoweredOn = false
            for await state in states {
                if state == .poweredOn {
                    sawPoweredOn = true
                } else if await self?.adapterLeftPoweredOn(attempt: id, watchSawPoweredOn: sawPoweredOn) ?? true {
                    return
                }
            }
        }
    }

    /// FCB registers the watch's state stream asynchronously, so a power-off just
    /// after `waitForPoweredOn` returned can arrive as the stream's REPLAY — the
    /// watch then never saw `.poweredOn` itself. The attempt having passed its
    /// powered-on gate counts as "was powered on". Returns whether the watch is done.
    private func adapterLeftPoweredOn(attempt id: UInt64, watchSawPoweredOn: Bool) -> Bool {
        guard watchSawPoweredOn || attemptPassedPowerGate == id else { return false }
        guard connectionState == .connecting || connectionState == .configuring else { return true }
        logger.error("adapter left poweredOn during a connect attempt")
        abortAttempt(id, AdapterLeftPoweredOn())
        return true
    }

    private func check(_ myEpoch: UInt64, attempt: UInt64) throws {
        if myEpoch != epoch || attempt != currentAttempt || Task.isCancelled { throw CancellationError() }
    }

    private func attemptConnect(radio: UUID, epoch myEpoch: UInt64, attempt: UInt64) async throws {
        try check(myEpoch, attempt: attempt)
        myNodeNum = nil
        modemPreset = nil
        // connect() already emitted it for its first attempt; reconnect attempts emit it here.
        if connectionState != .connecting { setState(.connecting) }
        try await waitForPoweredOn(epoch: myEpoch, attempt: attempt)
        attemptPassedPowerGate = attempt
        // Subscribe now, so FCB's (asynchronous) registration is done long before
        // the handshake ends; a later subscription could miss an immediate drop.
        let linkEvents = central.events()
        // retrievePeripherals (not scan) is the only way this session finds a radio. It also
        // re-stores the live peripheral object in FCB, replacing one CoreBluetooth invalidated
        // in a power cycle (FCB BluetoothCentralClient+Live.swift:271-275).
        let known = await central.retrievePeripherals([radio])
        try check(myEpoch, attempt: attempt)
        guard known.contains(where: { $0.id == radio }) else {
            throw TerminalFailure(reason: "radio not found")
        }
        _ = try await central.connect(radio, ConnectOptions?.none)
        try check(myEpoch, attempt: attempt)
        _ = try await central.discoverServices(radio, [MeshGATT.serviceUUID])
        try check(myEpoch, attempt: attempt)
        _ = try await central.discoverCharacteristics(
            radio, MeshGATT.serviceUUID,
            [MeshGATT.toRadioUUID, MeshGATT.fromRadioUUID, MeshGATT.fromNumUUID]
        )
        try check(myEpoch, attempt: attempt)
        // Subscribe to FromNum BEFORE want_config so no notify is lost.
        try await central.setNotify(true, radio, MeshGATT.fromNumUUID)
        try check(myEpoch, attempt: attempt)
        startFromNumLoop(radio: radio, epoch: myEpoch)

        isDraining = true
        drainGeneration &+= 1
        let claimed = drainGeneration
        setState(.configuring)
        let timeout = startHandshakeTimeout(epoch: myEpoch, attempt: attempt)
        defer { timeout.cancel() }
        do {
            try await requestConfig(radio: radio)
            try await drainUntilConfigComplete(radio: radio, epoch: myEpoch, attempt: attempt)
            try check(myEpoch, attempt: attempt)
        } catch {
            if drainGeneration == claimed { isDraining = false }
            throw error
        }
        isDraining = false
        setState(.connected)
        emit(.configured(myNodeNum: myNodeNum ?? 0, modemPreset: modemPreset))
        startDisconnectWatch(radio: radio, epoch: myEpoch, events: linkEvents)
        // The handshake held the drain lock, so a FromNum notify during it was
        // dropped, and firmware also queues packets heard while no phone was
        // connected: drain whatever is left after config_complete now.
        Task { await self.drainSteady(radio: radio, epoch: myEpoch) }
    }

    /// CoreBluetooth silently drops commands issued before `.poweredOn`, and
    /// `retrievePeripherals` returns [] until then — so wait. FCB's state stream
    /// replays the current state, so this returns at once when already on.
    private func waitForPoweredOn(epoch myEpoch: UInt64, attempt: UInt64) async throws {
        var reportedOff = false
        for await state in central.state() {
            try check(myEpoch, attempt: attempt)
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
        let generation = try await acquireWriteSlot()
        defer { releaseWriteSlot(generation) }
        writeSeq &+= 1
        let seq = writeSeq
        let central = self.central
        try await withCheckedThrowingContinuation { (completion: CheckedContinuation<Void, any Error>) in
            inFlightWrite = (seq, completion)
            Task {   // actor-isolated: completes on the session
                let result: Result<Void, any Error>
                do {
                    try await central.writeValue(bytes, radio, MeshGATT.toRadioUUID, .withResponse)
                    result = .success(())
                } catch {
                    result = .failure(error)
                }
                self.finishWrite(seq, result)
            }
        }
    }

    /// FCB answered write `seq`. Ignored if teardown already failed it.
    private func finishWrite(_ seq: UInt64, _ result: Result<Void, any Error>) {
        guard let write = inFlightWrite, write.seq == seq else { return }
        inFlightWrite = nil
        write.completion.resume(with: result)
    }

    /// Returns the generation the slot was acquired in. A queued waiter is either
    /// thrown `notConnected` by teardown, or handed the slot together with the
    /// generation it was handed in. If a teardown lands between that hand-off and
    /// the waiter resuming, the generations differ and the waiter throws
    /// `notConnected` too — its slot was already freed. So a queued write never
    /// runs unslotted or on a newer link, and a queued handshake write fails its
    /// attempt retryably.
    private func acquireWriteSlot() async throws -> UInt64 {
        guard writeInFlight else {
            writeInFlight = true
            return writeGeneration
        }
        let handedIn = try await withCheckedThrowingContinuation { writeWaiters.append($0) }
        guard handedIn == writeGeneration else { throw MeshtasticSessionError.notConnected }
        return handedIn
    }

    /// Hands the slot straight to the next waiter (it stays `writeInFlight`),
    /// stamped with the current generation. A writer from an older generation
    /// releases nothing: its slot was already freed by `resetWriteSlot()` and may
    /// now belong to a newer writer.
    private func releaseWriteSlot(_ generation: UInt64) {
        guard generation == writeGeneration else { return }
        if writeWaiters.isEmpty {
            writeInFlight = false
        } else {
            writeWaiters.removeFirst().resume(returning: writeGeneration)
        }
    }

    /// Link teardown: fail the in-flight write now (FCB may never resume it — its
    /// caller must not hang), free the slot, and fail every queued writer.
    private func resetWriteSlot() {
        if let write = inFlightWrite {
            inFlightWrite = nil
            write.completion.resume(throwing: MeshtasticSessionError.notConnected)
        }
        writeGeneration &+= 1
        writeInFlight = false
        let waiters = writeWaiters
        writeWaiters = []
        for waiter in waiters { waiter.resume(throwing: MeshtasticSessionError.notConnected) }
    }

    /// Reads FromRadio until config_complete_id == nonce. Empty reads are a
    /// transient gap during the handshake and are retried; malformed frames skipped.
    private func drainUntilConfigComplete(radio: UUID, epoch myEpoch: UInt64, attempt: UInt64) async throws {
        while true {
            let data = try await central.readValue(radio, MeshGATT.fromRadioUUID)
            try check(myEpoch, attempt: attempt)
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
        guard myEpoch == epoch, !Task.isCancelled else { return }
        guard !isDraining else {
            // Not dropped: the running drain (or the handshake, whose post-configure
            // drain follows) may already have read past this notify's frame.
            drainRequested = true
            return
        }
        isDraining = true
        drainRequested = false
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
            guard myEpoch == epoch, drainGeneration == claimed else { return }
            if data.isEmpty {
                // A notify arrived while this drain held the lock: read again before
                // releasing it, or its frame could sit until the next notify.
                guard drainRequested else { return }
                drainRequested = false
                continue
            }
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
        case .packet(let packet)?:
            emitPacketEvents(packet)
        default:
            break
        }
    }

    /// `.packet` for any decoded port; encrypted packets (no `.decoded`) are not
    /// emitted. A negative wire portnum (`PortNum.UNRECOGNIZED(-1)`) is dropped —
    /// `UInt32(_:)` would trap on it. ROUTING_APP also yields `.routing`.
    private func emitPacketEvents(_ packet: MeshPacket) {
        guard case .decoded(let data)? = packet.payloadVariant,
              let portnum = UInt32(exactly: data.portnum.rawValue) else { return }
        emit(.packet(from: packet.from, to: packet.to, channel: packet.channel,
                     portnum: portnum, payload: data.payload, id: packet.id))
        if data.portnum == .routingApp, let routing = try? Routing(serializedBytes: data.payload) {
            emit(.routing(packetID: data.requestID, ok: routing.errorReason == .none))
        }
    }

    // MARK: - Reconnect

    /// FCB's `events()` is process-global: only THIS radio's didDisconnect counts.
    /// A non-poweredOn state counts too — iOS sends no didDisconnect on power-off.
    private func startDisconnectWatch(radio: UUID, epoch myEpoch: UInt64, events: AsyncStream<CentralEvent>) {
        disconnectWatchTask?.cancel()
        disconnectWatchTask = Task { [weak self] in
            for await event in events {
                switch event {
                case .didDisconnect(let peripheral, _) where peripheral.id == radio:
                    await self?.handleLinkLost(radio: radio, epoch: myEpoch)
                    return
                case .didUpdateState(let state) where state != .poweredOn:
                    await self?.handleLinkLost(radio: radio, epoch: myEpoch)
                    return
                default:
                    continue
                }
            }
        }
    }

    private func handleLinkLost(radio: UUID, epoch myEpoch: UInt64) {
        guard myEpoch == epoch, connectionState == .connected else { return }
        disconnectWatchTask = nil
        fromNumTask?.cancel()
        fromNumTask = nil
        // A power-off fails no FCB op and sends no didDisconnect: a write in
        // flight now never returns. Do not let it hold the slot for the next link.
        resetWriteSlot()
        scheduleReconnect(radio: radio, epoch: myEpoch)
    }

    private func scheduleReconnect(radio: UUID, epoch myEpoch: UInt64) {
        setState(.reconnecting(attempt: 1))
        reconnectTask?.cancel()
        reconnectTask = Task { [weak self] in
            await self?.reconnectLoop(radio: radio, epoch: myEpoch)
        }
    }

    /// Capped exponential backoff — 1 s, 2 s, 4 s, 8 s, 16 s, then 30 s forever —
    /// on the injected clock, until an attempt connects, a terminal cause appears,
    /// or connect/disconnect bumps the epoch. Never scans: attempts retrieve.
    private func reconnectLoop(radio: UUID, epoch myEpoch: UInt64) async {
        var delay: Duration = .seconds(1)
        var attempt = 1
        while myEpoch == epoch {
            do {
                try await clock.sleep(for: delay)
            } catch {
                return
            }
            guard myEpoch == epoch else { return }
            switch await runAttempt(radio: radio, epoch: myEpoch) {
            case .connected, .superseded:
                return
            case .terminal(let reason):
                radioID = nil
                setState(.disconnected(reason: reason))
                return
            case .retryable(let reason):
                logger.error("reconnect attempt \(attempt) failed: \(reason)")
                delay = min(delay * 2, Self.maxBackoff)
                attempt += 1
                setState(.reconnecting(attempt: attempt))
            }
        }
    }

    /// No config_complete within `handshakeTimeout` on the injected clock → the
    /// session fails the attempt itself (retryably), abandoning the handshake's
    /// pending FCB read; `runAttempt`'s failure path then drops the BLE link and
    /// the caller enters backoff — the same path as a lost link. It does not wait
    /// for FCB to fail the read (older FCB never did after a power-off).
    /// Cancelled by `attemptConnect`'s defer when the handshake ends.
    private func startHandshakeTimeout(epoch myEpoch: UInt64, attempt: UInt64) -> Task<Void, Never> {
        let clock = self.clock
        return Task { [weak self] in
            do {
                try await clock.sleep(for: Self.handshakeTimeout)
            } catch {
                return
            }
            await self?.handshakeTimedOut(epoch: myEpoch, attempt: attempt)
        }
    }

    private func handshakeTimedOut(epoch myEpoch: UInt64, attempt: UInt64) {
        guard myEpoch == epoch, connectionState == .configuring else { return }
        logger.error("want_config handshake timed out")
        abortAttempt(attempt, HandshakeTimedOut())
    }
}
