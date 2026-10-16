@preconcurrency import CoreBluetooth
@preconcurrency import CoreBluetoothMock
import Foundation
import MeshProto
import MeshtasticKit

/// A scripted Meshtastic radio implemented as a CoreBluetoothMock peripheral.
/// Serves a canned node DB (plus a LoRa config carrying `modemPreset`) on
/// want_config, routing-replies to every packet written to it on ANY port per
/// `ackBehavior`, and records those packets — real protobufs over the
/// FromRadio (read) / FromNum (notify) / ToRadio (write) contract.
/// Usable in integration tests and in the iOS Simulator.
public final class MockMeshtasticDevice: @unchecked Sendable {
    public static let selfNodeNum: UInt32 = 0xA0A0

    /// Canned node numbers, in emission order.
    public static let nodeNums: [UInt32] = [0xB001, 0xB002, 0xB003, 0xB004, 0xB005, 0xB006]
    /// Seconds-ago offsets for each node's last_heard (first three < 2h).
    static let lastHeardOffsets: [TimeInterval] = [60, 600, 3600, 7201, 86_400, 172_800]
    static let batteryLevels: [UInt32] = [95, 80, 60, 40, 20, 5]

    /// How the radio answers a written packet.
    public enum AckBehavior: Equatable, Sendable {
        /// Routing reply with error_reason NONE (the default).
        case ack
        /// Routing reply carrying this error reason.
        case nak(Routing.Error)
        /// No routing reply at all.
        case silent
    }

    private let delegate: Delegate

    public init(now: @escaping () -> Date = Date.init,
                modemPreset: Config.LoRaConfig.ModemPreset? = .longFast) {
        self.delegate = Delegate(now: now, modemPreset: modemPreset)
    }

    /// The preset served in the LoRa config frame of the NEXT want_config
    /// handshake; nil serves no LoRa config frame at all.
    public var modemPreset: Config.LoRaConfig.ModemPreset? {
        get { delegate.withState { $0.modemPreset } }
        set { delegate.withState { $0.modemPreset = newValue } }
    }

    public var ackBehavior: AckBehavior {
        get { delegate.withState { $0.ackBehavior } }
        set { delegate.withState { $0.ackBehavior = newValue } }
    }

    /// Every decoded MeshPacket written to ToRadio, in arrival order.
    public var receivedPackets: [MeshPacket] { delegate.withState { $0.receivedPackets } }

    public func makeSpec(identifier: UUID) -> CBMPeripheralSpec {
        let toRadio = CBMCharacteristicMock(type: CBMUUID(string: MeshGATT.toRadioUUID.uuidString), properties: [.write])
        let fromRadio = CBMCharacteristicMock(type: CBMUUID(string: MeshGATT.fromRadioUUID.uuidString), properties: [.read])
        let fromNum = CBMCharacteristicMock(type: CBMUUID(string: MeshGATT.fromNumUUID.uuidString), properties: [.notify])
        delegate.fromNumCharacteristic = fromNum
        let service = CBMServiceMock(
            type: CBMUUID(string: MeshGATT.serviceUUID.uuidString),
            primary: true,
            characteristics: toRadio, fromRadio, fromNum
        )
        let spec = CBMPeripheralSpec
            .simulatePeripheral(identifier: identifier, proximity: .near)
            .advertising(advertisementData: [
                CBMAdvertisementDataLocalNameKey: "Meshtastic_MOCK",
                CBMAdvertisementDataServiceUUIDsKey: [CBMUUID(string: MeshGATT.serviceUUID.uuidString)],
                CBMAdvertisementDataIsConnectable: true as NSNumber,
            ], withInterval: 0.05)
            .connectable(name: "Meshtastic_MOCK", services: [service], delegate: delegate, connectionInterval: 0.01)
            .allowForRetrieval()
            .build()
        delegate.spec = spec
        return spec
    }

    /// Enqueue an incoming text (broadcast by default) and fire FromNum so the client drains it.
    public func injectIncomingText(_ text: String, from nodeNum: UInt32, to: UInt32 = 0xFFFF_FFFF) {
        injectIncomingPacket(portnum: .textMessageApp, payload: Data(text.utf8), from: nodeNum, to: to)
    }

    /// Enqueue a decoded incoming packet on any port and fire FromNum.
    public func injectIncomingPacket(portnum: PortNum, payload: Data, from nodeNum: UInt32,
                                     to: UInt32 = 0xFFFF_FFFF, channel: UInt32 = 0,
                                     id: UInt32 = UInt32.random(in: 1...UInt32.max)) {
        var data = DataMessage()
        data.portnum = portnum
        data.payload = payload
        var packet = MeshPacket()
        packet.from = nodeNum
        packet.to = to
        packet.channel = channel
        packet.id = id
        packet.rxTime = delegate.rxTimeNow()
        packet.decoded = data
        var frame = FromRadio()
        frame.packet = packet
        injectFrame(frame)
    }

    /// Enqueue any FromRadio frame and fire FromNum.
    public func injectFrame(_ frame: FromRadio) {
        delegate.enqueue(frame)
        delegate.pushFromNum()
    }

    /// Enqueue arbitrary bytes as the next FromRadio read. `notify: false` queues
    /// without firing FromNum, so a following inject drains both in ONE drain.
    public func injectRawFrame(_ bytes: Data, notify: Bool = true) {
        delegate.enqueueRaw(bytes)
        if notify { delegate.pushFromNum() }
    }

    // MARK: - Periodic traffic

    private let trafficLock = NSLock()
    private var trafficTask: Task<Void, Never>?

    private static let cannedTexts: [String] = [
        "Anyone on channel?",
        "Mesh link looks good 👍",
        "Battery holding steady",
        "Copy that, over",
        "Heading to grid square 7",
    ]

    /// Spawn a retained `Task` that periodically injects a canned incoming
    /// message from one of the "online" fake nodes (0xB001–0xB003), cycling
    /// through both texts and senders. Idempotent (cancels a previous task first).
    public func startPeriodicTraffic(interval: Duration = .seconds(25)) {
        trafficLock.lock()
        trafficTask?.cancel()
        let task = Task {
            let clock = ContinuousClock()
            let onlineNodes: [UInt32] = [0xB001, 0xB002, 0xB003]
            let texts = Self.cannedTexts
            var index = 0
            while !Task.isCancelled {
                try? await clock.sleep(for: interval)
                guard !Task.isCancelled else { break }
                injectIncomingText(texts[index % texts.count], from: onlineNodes[index % onlineNodes.count])
                index += 1
            }
        }
        trafficTask = task
        trafficLock.unlock()
    }

    public func stopPeriodicTraffic() {
        trafficLock.lock()
        trafficTask?.cancel()
        trafficTask = nil
        trafficLock.unlock()
    }

    deinit {
        trafficTask?.cancel()
    }

    // MARK: - Spec delegate

    final class Delegate: CBMPeripheralSpecDelegate, @unchecked Sendable {
        struct MutableState {
            var queue: [Data] = []
            var modemPreset: Config.LoRaConfig.ModemPreset?
            var ackBehavior: AckBehavior = .ack
            var receivedPackets: [MeshPacket] = []
        }

        private let lock = NSLock()
        private var state: MutableState
        private let now: () -> Date
        var fromNumCharacteristic: CBMCharacteristicMock?
        weak var spec: CBMPeripheralSpec?

        init(now: @escaping () -> Date, modemPreset: Config.LoRaConfig.ModemPreset?) {
            self.now = now
            self.state = MutableState(modemPreset: modemPreset)
        }

        /// Explicit lock/unlock: `NSLock.withLock` is macOS 14+, and this package's floor is macOS 13.
        func withState<T>(_ body: (inout MutableState) -> T) -> T {
            lock.lock()
            defer { lock.unlock() }
            return body(&state)
        }

        func enqueue(_ frame: FromRadio) {
            if let data = try? frame.serializedData() { enqueueRaw(data) }
        }

        func enqueueRaw(_ bytes: Data) {
            withState { $0.queue.append(bytes) }
        }

        func pushFromNum() {
            if let ch = fromNumCharacteristic {
                spec?.simulateValueUpdate(Data([0x01]), for: ch)
            }
        }

        func rxTimeNow() -> UInt32 { UInt32(now().timeIntervalSince1970) }

        private func selfMyInfo() -> FromRadio {
            var my = MyNodeInfo()
            my.myNodeNum = MockMeshtasticDevice.selfNodeNum
            my.nodedbCount = UInt32(MockMeshtasticDevice.nodeNums.count + 1)
            var fr = FromRadio()
            fr.myInfo = my
            return fr
        }

        private func selfNodeFrame() -> FromRadio {
            var user = User()
            user.longName = "Mesh Base"
            user.shortName = "BASE"
            var info = NodeInfo()
            info.num = MockMeshtasticDevice.selfNodeNum
            info.user = user
            info.lastHeard = UInt32(now().timeIntervalSince1970 - 30)
            var fr = FromRadio()
            fr.nodeInfo = info
            return fr
        }

        private func nodeFrame(index: Int) -> FromRadio {
            let num = MockMeshtasticDevice.nodeNums[index]
            var user = User()
            user.longName = "Node \(index + 1)"
            user.shortName = "N\(index + 1)"
            var metrics = DeviceMetrics()
            metrics.batteryLevel = MockMeshtasticDevice.batteryLevels[index]
            metrics.channelUtilization = 10
            metrics.airUtilTx = 2
            var info = NodeInfo()
            info.num = num
            info.user = user
            info.snr = 5
            info.lastHeard = UInt32(now().timeIntervalSince1970 - MockMeshtasticDevice.lastHeardOffsets[index])
            info.deviceMetrics = metrics
            var fr = FromRadio()
            fr.nodeInfo = info
            return fr
        }

        private func loraConfigFrame(_ preset: Config.LoRaConfig.ModemPreset) -> FromRadio {
            var lora = Config.LoRaConfig()
            lora.usePreset = true
            lora.modemPreset = preset
            lora.hopLimit = 3
            var config = Config()
            config.lora = lora
            var fr = FromRadio()
            fr.config = config
            return fr
        }

        private func metadataFrame() -> FromRadio {
            var meta = DeviceMetadata()
            meta.firmwareVersion = "2.5.0.mock"
            var fr = FromRadio()
            fr.metadata = meta
            return fr
        }

        private func channelFrame() -> FromRadio {
            var settings = ChannelSettings()
            settings.name = ""
            var channel = Channel()
            channel.index = 0
            channel.role = .primary
            channel.settings = settings
            var fr = FromRadio()
            fr.channel = channel
            return fr
        }

        private func routingReply(to sent: MeshPacket, behavior: AckBehavior) -> FromRadio? {
            var routing = Routing()
            switch behavior {
            case .silent: return nil
            case .ack: routing.errorReason = .none
            case .nak(let reason): routing.errorReason = reason
            }
            var ackData = DataMessage()
            ackData.portnum = .routingApp
            ackData.requestID = sent.id
            ackData.payload = (try? routing.serializedData()) ?? Data()
            var ackPacket = MeshPacket()
            ackPacket.from = MockMeshtasticDevice.selfNodeNum
            ackPacket.to = MockMeshtasticDevice.selfNodeNum
            ackPacket.decoded = ackData
            var ackFrame = FromRadio()
            ackFrame.packet = ackPacket
            return ackFrame
        }

        /// Demo value: a text DM to one of the fake nodes gets a canned reply FROM
        /// that node addressed to selfNodeNum, ~3 s later.
        private func scheduleCannedReply(from dest: UInt32) {
            let now = self.now
            Task { [weak self] in
                try? await Task.sleep(nanoseconds: 3_000_000_000)
                guard let self else { return }
                var reply = DataMessage()
                reply.portnum = .textMessageApp
                reply.payload = Data("Got your message 👋".utf8)
                var packet = MeshPacket()
                packet.from = dest
                packet.to = MockMeshtasticDevice.selfNodeNum
                packet.channel = 0
                packet.id = UInt32.random(in: 1...UInt32.max)
                packet.rxTime = UInt32(now().timeIntervalSince1970)
                packet.decoded = reply
                var fr = FromRadio()
                fr.packet = packet
                self.enqueue(fr)
                self.pushFromNum()
            }
        }

        // MARK: CBMPeripheralSpecDelegate

        func peripheral(_ peripheral: CBMPeripheralSpec,
                        didReceiveReadRequestFor characteristic: CBMCharacteristicMock) -> Result<Data, any Error> {
            guard characteristic.uuid.uuidString == MeshGATT.fromRadioUUID.uuidString else {
                return .success(Data())
            }
            let next = withState { s in s.queue.isEmpty ? Data() : s.queue.removeFirst() }
            return .success(next)
        }

        func peripheral(_ peripheral: CBMPeripheralSpec,
                        didReceiveWriteRequestFor characteristic: CBMCharacteristicMock,
                        data: Data) -> Result<Void, any Error> {
            guard characteristic.uuid.uuidString == MeshGATT.toRadioUUID.uuidString,
                  let toRadio = try? ToRadio(serializedBytes: data) else {
                return .success(())
            }
            switch toRadio.payloadVariant {
            case .wantConfigID(let nonce)?:
                let preset = withState { $0.modemPreset }
                var frames = [selfMyInfo(), selfNodeFrame()]
                frames += MockMeshtasticDevice.nodeNums.indices.map { nodeFrame(index: $0) }
                if let preset { frames.append(loraConfigFrame(preset)) }
                frames += [metadataFrame(), channelFrame()]
                var complete = FromRadio()
                complete.configCompleteID = nonce
                frames.append(complete)
                for frame in frames { enqueue(frame) }
                pushFromNum()
            case .packet(let sent)?:
                guard case .decoded(let sentData)? = sent.payloadVariant else { return .success(()) }
                let behavior = withState { s -> AckBehavior in
                    s.receivedPackets.append(sent)
                    return s.ackBehavior
                }
                if let reply = routingReply(to: sent, behavior: behavior) {
                    enqueue(reply)
                    pushFromNum()
                }
                if sentData.portnum == .textMessageApp, MockMeshtasticDevice.nodeNums.contains(sent.to) {
                    scheduleCannedReply(from: sent.to)
                }
            default:
                break
            }
            return .success(())
        }

        func peripheral(_ peripheral: CBMPeripheralSpec,
                        didReceiveSetNotifyRequest enabled: Bool,
                        for characteristic: CBMCharacteristicMock) -> Result<Void, any Error> {
            .success(())
        }
    }
}
