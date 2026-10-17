import ComposableArchitecture
@preconcurrency import CoreBluetooth
@preconcurrency import CoreBluetoothMock
import FCBComposable
import Foundation
import MeshProto
import MeshtasticKit
import MeshtasticMock
import Testing

private func failingConnect(_ id: UUID) -> @Sendable () async throws -> Void {
    { throw BluetoothError.connectionFailed(peripheralID: id, underlying: nil) }
}

struct SessionBackoffTests {
    @Test func firstAttemptFailureEntersBackoffInsteadOfStopping() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let clock = TestClock()   // holds the loop in its first sleep
        let session = MeshtasticSession(central: fakeCentral(radio: id, log: log, connect: failingConnect(id)), clock: clock)
        let events = session.events()
        await session.connect(radio: id)
        let (seen, matched) = await collect(events, timeout: .seconds(2)) { $0.connectionState == .reconnecting(attempt: 1) }
        await session.disconnect()
        #expect(matched)
        #expect(seen.compactMap(\.connectionState) == [.disconnected(reason: nil), .connecting, .reconnecting(attempt: 1)])
    }

    @Test func backoffDoublesFromOneSecondAndCapsAtThirty() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let clock = SleepRecordingClock()
        let session = MeshtasticSession(central: fakeCentral(radio: id, log: log, connect: failingConnect(id)), clock: clock)
        await session.connect(radio: id)
        let reached = await waitUntil(timeout: .seconds(5)) { clock.sleeps.value.count >= 8 }
        await session.disconnect()
        #expect(reached)
        #expect(Array(clock.sleeps.value.prefix(8)) == [1, 2, 4, 8, 16, 30, 30, 30].map { Duration.seconds($0) })
    }

    @Test func reconnectingCountsAttemptsUpFromOne() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let session = MeshtasticSession(central: fakeCentral(radio: id, log: log, connect: failingConnect(id)),
                                        clock: SleepRecordingClock())
        let events = session.events()
        await session.connect(radio: id)
        let (seen, matched) = await collect(events) { $0.connectionState == .reconnecting(attempt: 3) }
        await session.disconnect()
        #expect(matched)
        let attempts = seen.compactMap { event -> Int? in
            if case .reconnecting(let n)? = event.connectionState { return n }
            return nil
        }
        #expect(attempts == [1, 2, 3])
    }

    @Test func attemptResolvingAfterDisconnectStaysDisconnected() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let gate = AsyncGate()
        let central = fakeCentral(radio: id, log: log, connect: {
            await gate.wait()
            throw BluetoothError.connectionFailed(peripheralID: id, underlying: nil)
        })
        let session = MeshtasticSession(central: central, clock: ImmediateClock())
        let connecting = Task { await session.connect(radio: id) }
        let started = await waitUntil { log.value.contains("connect") }
        #expect(started)
        await session.disconnect()
        let after = session.events()
        await gate.open()
        await connecting.value
        let (seen, _) = await collect(after, timeout: .milliseconds(500)) { _ in false }
        #expect(seen.compactMap(\.connectionState) == [.disconnected(reason: nil)])
    }

    /// A fake whose FromRadio read suspends until `gate` opens, ignoring
    /// cancellation and disconnect — a dependency that never answers (older FCB
    /// did this for a read pending at a power-off). CBM cannot hold a read of a
    /// CONNECTED peripheral open, hence the fake.
    private func gatedReadCentral(radio: UUID, log: LockIsolated<[String]>, gate: AsyncGate,
                                  lateFrame: Data = Data()) -> BluetoothCentralClient {
        var central = fakeCentral(radio: radio, log: log, connect: {})
        central.readValue = { _, _ in
            await gate.wait()
            return lateFrame
        }
        return central
    }

    /// connect() superseded by disconnect() while it is still tearing down the
    /// previous link — before its attempt task ever runs — must still have emitted
    /// `.connecting` ("always emits .connecting first"; Meshtastified's adapter
    /// waits for it). The fake parks connect's teardown in `central.disconnect`.
    @Test func connectSupersededBeforeItsAttemptStillEmitsConnecting() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let disconnectGate = AsyncGate()
        var central = gatedReadCentral(radio: id, log: log, gate: AsyncGate())
        central.disconnect = { _ in
            log.withValue { $0.append("disconnect") }
            await disconnectGate.wait()
        }
        let session = MeshtasticSession(central: central, clock: TestClock())
        let first = session.events()
        Task { await session.connect(radio: id) }   // the previous link: held in its handshake
        let (_, configuring) = await collect(first) { $0 == .connection(.configuring) }
        #expect(configuring)
        let after = session.events()                 // replays .configuring
        Task { await session.connect(radio: id) }   // parks in teardown's central.disconnect
        let parked = await waitUntil(timeout: .seconds(2)) { log.value.contains("disconnect") }
        #expect(parked)
        await session.disconnect()
        await disconnectGate.open()
        let (seen, _) = await collect(after, timeout: .milliseconds(300)) { _ in false }
        #expect(seen.compactMap(\.connectionState) == [.configuring, .connecting, .disconnected(reason: nil)])
    }

    @Test func handshakeTimeoutFailsTheAttemptEvenIfItsReadNeverResolves() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let clock = TestClock()
        let session = MeshtasticSession(central: gatedReadCentral(radio: id, log: log, gate: AsyncGate()), clock: clock)
        let events = session.events()
        // Unstructured and never awaited: a session waiting on the read would hang `connect`.
        Task { await session.connect(radio: id) }
        let (_, configuring) = await collect(events) { $0 == .connection(.configuring) }
        #expect(configuring)
        try? await Task.sleep(for: .milliseconds(100))   // let the timeout's sleep register on the TestClock
        let after = session.events()
        await clock.advance(by: .seconds(30))
        let (_, backedOff) = await collect(after) { $0 == .connection(.reconnecting(attempt: 1)) }
        #expect(backedOff)
        await session.disconnect()
    }

    /// Attempt 1's want_config write is abandoned by the handshake timeout; attempt
    /// 2's write is then in flight when attempt 1's FCB write finally fails. That
    /// late result belongs to the abandoned write and must not fail attempt 2.
    @Test func lateResultOfAnAbandonedWriteCannotCompleteTheCurrentOne() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let clock = TestClock()
        let firstWriteGate = AsyncGate()
        let writes = LockIsolated(0)
        var central = fakeCentral(radio: id, log: log, connect: {})
        central.writeValue = { _, _, _, _ in
            let n = writes.withValue { count -> Int in count += 1; return count }
            if n == 1 {
                await firstWriteGate.wait()
                throw BluetoothError.operationCancelled
            }
            await AsyncGate().wait()   // every later write stays in flight
        }
        let session = MeshtasticSession(central: central, clock: clock)
        let events = session.events()
        Task { await session.connect(radio: id) }
        let (_, configuring) = await collect(events) { $0 == .connection(.configuring) }
        #expect(configuring)
        try? await Task.sleep(for: .milliseconds(100))
        let firstAbort = session.events()
        await clock.advance(by: .seconds(30))   // attempt 1's handshake times out
        let (_, backedOff) = await collect(firstAbort) { $0 == .connection(.reconnecting(attempt: 1)) }
        #expect(backedOff)
        try? await Task.sleep(for: .milliseconds(100))
        let second = session.events()
        await clock.advance(by: .seconds(1))    // backoff ends: attempt 2
        let (_, secondConfiguring) = await collect(second) { $0 == .connection(.configuring) }
        #expect(secondConfiguring)
        let secondWriteIssued = await waitUntil(timeout: .seconds(2)) { writes.value == 2 }
        #expect(secondWriteIssued)
        let late = session.events()
        await firstWriteGate.open()             // attempt 1's write fails, late
        let (seen, _) = await collect(late, timeout: .milliseconds(500)) { _ in false }
        #expect(seen == [.connection(.configuring)])
        await session.disconnect()
    }

    @Test func disconnectEndsAConnectStuckInItsHandshake() async {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let session = MeshtasticSession(central: gatedReadCentral(radio: id, log: log, gate: AsyncGate()), clock: TestClock())
        let events = session.events()
        let returned = LockIsolated(false)
        Task {
            await session.connect(radio: id)
            returned.setValue(true)
        }
        let (_, configuring) = await collect(events) { $0 == .connection(.configuring) }
        #expect(configuring)
        let after = session.events()
        await session.disconnect()
        let connectReturned = await waitUntil(timeout: .seconds(2)) { returned.value }
        #expect(connectReturned)
        let (seen, _) = await collect(after, timeout: .milliseconds(300)) { _ in false }
        #expect(seen.compactMap(\.connectionState) == [.configuring, .disconnected(reason: nil)])
    }

    @Test func abandonedHandshakeResolvingLateCannotResurrectTheLink() async throws {
        let id = UUID()
        let log = LockIsolated<[String]>([])
        let clock = TestClock()   // never advanced past the timeout: the loop stays in its first backoff sleep
        let gate = AsyncGate()
        var complete = FromRadio()
        complete.configCompleteID = 7
        let central = gatedReadCentral(radio: id, log: log, gate: gate, lateFrame: try complete.serializedData())
        let session = MeshtasticSession(central: central, clock: clock, packetID: { 7 })
        let events = session.events()
        Task { await session.connect(radio: id) }
        let (_, configuring) = await collect(events) { $0 == .connection(.configuring) }
        #expect(configuring)
        try? await Task.sleep(for: .milliseconds(100))
        let aborted = session.events()
        await clock.advance(by: .seconds(30))
        let (_, backedOff) = await collect(aborted) { $0 == .connection(.reconnecting(attempt: 1)) }
        #expect(backedOff)
        // The abandoned handshake's read now returns a matching config_complete.
        let late = session.events()
        await gate.open()
        let (seen, _) = await collect(late, timeout: .milliseconds(500)) { _ in false }
        #expect(seen == [.connection(.reconnecting(attempt: 1))])
        await session.disconnect()
    }
}

extension CBMBackedTests {
    @Suite @MainActor
    struct SessionReconnectTests {
        @Test func unexpectedDisconnectReconnectsViaRetrieveAndNeverScans() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            h.log.setValue([])
            let events = h.session.events()
            h.radio.spec.simulateDisconnection()
            let (seen, matched) = await collect(events, timeout: .seconds(10)) { $0.isConfigured }
            #expect(matched)
            #expect(seen.contains { $0.connectionState == .reconnecting(attempt: 1) })
            #expect(h.log.value.contains("retrievePeripherals"))
            #expect(!h.log.value.contains("scan"))
        }

        @Test func reconnectReemitsConfiguredWithTheRadiosCurrentPreset() async {
            let h = await makeSessionHarness(modemPreset: .shortFast)
            await h.session.connect(radio: h.radio.id)
            h.radio.device.modemPreset = .longFast
            let events = h.session.events()
            h.radio.spec.simulateDisconnection()
            let (seen, _) = await collect(events, timeout: .seconds(10)) { $0.isConfigured }
            #expect(seen.last == .configured(myNodeNum: MockMeshtasticDevice.selfNodeNum,
                                                                   modemPreset: Config.LoRaConfig.ModemPreset.longFast.rawValue))
        }

        @Test func anotherPeripheralsDisconnectIsIgnored() async throws {
            let otherID = UUID()
            let otherSpec = MockMeshtasticDevice().makeSpec(identifier: otherID)
            let h = await makeSessionHarness(extra: [otherSpec])
            await h.session.connect(radio: h.radio.id)
            _ = await h.radio.central.retrievePeripherals([otherID])
            _ = try await h.radio.central.connect(otherID, ConnectOptions?.none)
            let quiet = h.session.events()
            otherSpec.simulateDisconnection()
            let (seen, _) = await collect(quiet, timeout: .seconds(1)) { _ in false }
            #expect(!seen.contains { event in
                if case .reconnecting? = event.connectionState { return true }
                return false
            })
            // The watch is alive: our own radio's drop IS seen.
            let loud = h.session.events()
            h.radio.spec.simulateDisconnection()
            let (_, sawReconnect) = await collect(loud) { $0.connectionState == .reconnecting(attempt: 1) }
            #expect(sawReconnect)
        }

        @Test func disconnectDuringBackoffStaysDisconnected() async {
            let clock = TestClock()
            let h = await makeSessionHarness(clock: clock)
            await h.session.connect(radio: h.radio.id)
            let watching = h.session.events()
            h.radio.spec.simulateDisconnection()
            let (_, sawReconnecting) = await collect(watching) { $0.connectionState == .reconnecting(attempt: 1) }
            #expect(sawReconnecting)
            await h.session.disconnect()
            let later = h.session.events()
            for _ in 0..<20 {
                await clock.advance(by: .seconds(30))
                try? await Task.sleep(for: .milliseconds(100))
            }
            let (seen, _) = await collect(later, timeout: .milliseconds(200)) { _ in false }
            #expect(seen.compactMap(\.connectionState) == [.disconnected(reason: nil)])
        }

        /// iOS (and CBM, CBMCentralManagerMock.swift:80-104) report a power-off as a
        /// state change only — no didDisconnect. FCB fails pending operations at
        /// the power-off (lane F); the session must not depend on it.
        @Test func powerCycleWhileConnectedReconnectsAndReportsAdapter() async {
            let h = await makeSessionHarness()
            await h.session.connect(radio: h.radio.id)
            let off = h.session.events()
            CBMCentralManagerMock.simulatePowerOff()
            let (seenOff, sawUnavailable) = await collect(off) { $0.isAdapterUnavailable }
            #expect(sawUnavailable)
            #expect(seenOff.contains { $0.connectionState == .reconnecting(attempt: 1) })
            #expect(seenOff.filter(\.isAdapterUnavailable) == [.adapterUnavailable(.poweredOff)])
            let on = h.session.events()
            CBMCentralManagerMock.simulatePowerOn()
            let (_, reconfigured) = await collect(on, timeout: .seconds(10)) { $0.isConfigured }
            #expect(reconfigured)
        }

        /// A radio that never sends config_complete: still `.configuring` at 29.9 s on
        /// the TestClock, backing off by 30.1 s.
        @Test func handshakeWithoutConfigCompleteTimesOutAtThirtySecondsIntoBackoff() async {
            let clock = TestClock()
            let h = await makeSessionHarness(clock: clock)
            h.radio.device.answersWantConfig = false
            let events = h.session.events()
            let connecting = Task { await h.session.connect(radio: h.radio.id) }
            let (_, configuring) = await collect(events) { $0 == .connection(.configuring) }
            #expect(configuring)
            try? await Task.sleep(for: .milliseconds(100))   // let the timeout's sleep register on the TestClock
            await clock.advance(by: .milliseconds(29_900))
            try? await Task.sleep(for: .milliseconds(100))
            let (atDeadline, _) = await collect(h.session.events(), timeout: .seconds(1)) { _ in true }
            #expect(atDeadline.first == .connection(.configuring))
            let after = h.session.events()
            await clock.advance(by: .milliseconds(200))
            let (_, backedOff) = await collect(after) { $0 == .connection(.reconnecting(attempt: 1)) }
            #expect(backedOff)
            await h.session.disconnect()   // first: also ends a handshake that never timed out
            await connecting.value
        }

        /// Power-off DURING the want_config handshake: no didDisconnect. (Older FCB
        /// failed no pending op, so the handshake's FromRadio read never resolved;
        /// FCB now fails it with `.poweredOff` — the session copes either way.) The hook
        /// powers off INSIDE the first FromRadio read request (the radio ignores
        /// want_config, so the session is deterministically `.configuring` with that
        /// read pending). The session itself must fail the attempt, back off, and
        /// configure again once power returns. Real clock: 1 s + 2 s of backoff
        /// (older FCB's stale pending read cost one extra attempt; none now).
        @Test func powerOffDuringHandshakeFailsTheAttemptAndReconnects() async {
            let h = await makeSessionHarness()
            h.radio.device.answersWantConfig = false
            let armed = LockIsolated(true)
            h.radio.device.onFromRadioRead = { _ in
                let fire = armed.withValue { isArmed -> Bool in defer { isArmed = false }; return isArmed }
                if fire { CBMCentralManagerMock.simulatePowerOff() }
            }
            let off = h.session.events()
            // Unstructured: a session stuck on the leaked read would hang `connect`.
            Task { [session = h.session, id = h.radio.id] in await session.connect(radio: id) }
            let (seenOff, sawUnavailable) = await collect(off) { $0.isAdapterUnavailable }
            #expect(!armed.value)   // the scenario: powered off with a handshake read pending
            #expect(sawUnavailable)
            #expect(seenOff.compactMap(\.connectionState).contains(.configuring))
            #expect(seenOff.contains { $0.connectionState == .reconnecting(attempt: 1) })
            #expect(!seenOff.contains { $0.isConfigured })

            h.radio.device.answersWantConfig = true
            let on = h.session.events()
            CBMCentralManagerMock.simulatePowerOn()
            let (_, reconfigured) = await collect(on, timeout: .seconds(10)) { $0.isConfigured }
            #expect(reconfigured)
            h.radio.device.onFromRadioRead = nil
            await h.session.disconnect()
        }

        /// FCB registers a state stream asynchronously and replays the CURRENT state.
        /// If power goes off after the attempt passed its powered-on gate but before
        /// the attempt's power watch registered, the watch's first (and only) value is
        /// `.poweredOff`: it never saw `.poweredOn` itself. The wrapper (live FCB client
        /// underneath) holds the watch's subscription — the first `state()` call of
        /// the attempt, made by `runAttempt` before the attempt task runs — until the
        /// hook has powered off inside the first handshake read. Deterministic.
        @Test func powerOffBeforeThePowerWatchRegistersStillFailsTheAttempt() async {
            let radio = await makeMockRadio()
            let base = radio.central
            let stateCalls = LockIsolated(0)
            let watchMayRegister = AsyncGate()
            var central = base
            central.state = {
                let call = stateCalls.withValue { count -> Int in count += 1; return count }
                guard call == 1 else { return base.state() }
                return AsyncStream { continuation in
                    let forward = Task {
                        await watchMayRegister.wait()
                        for await state in base.state() { continuation.yield(state) }
                        continuation.finish()
                    }
                    continuation.onTermination = { _ in forward.cancel() }
                }
            }
            let session = MeshtasticSession(central: central)
            radio.device.answersWantConfig = false
            let armed = LockIsolated(true)
            radio.device.onFromRadioRead = { _ in
                let fire = armed.withValue { isArmed -> Bool in defer { isArmed = false }; return isArmed }
                guard fire else { return }
                CBMCentralManagerMock.simulatePowerOff()
                Task { await watchMayRegister.open() }
            }
            let off = session.events()
            // Unstructured: a session stuck on the leaked read would hang `connect`.
            Task { await session.connect(radio: radio.id) }
            let (seenOff, sawReconnecting) = await collect(off) { $0.connectionState == .reconnecting(attempt: 1) }
            #expect(!armed.value)   // the scenario: powered off with a handshake read pending
            #expect(seenOff.compactMap(\.connectionState).contains(.configuring))
            #expect(sawReconnecting)

            radio.device.answersWantConfig = true
            let on = session.events()
            CBMCentralManagerMock.simulatePowerOn()
            let (_, reconfigured) = await collect(on, timeout: .seconds(10)) { $0.isConfigured }
            #expect(reconfigured)
            radio.device.onFromRadioRead = nil
            await session.disconnect()
        }
    }
}
