import Foundation
import MeshProto
import MeshtasticKit
import Testing

struct PublicTypesTests {
    @Test func outboundDefaultsToBroadcastOnChannelZeroWithoutAckAndFirmwareHopLimit() {
        let outbound = MeshtasticOutbound(portnum: 76, payload: Data([1]))
        #expect(outbound.to == 0xFFFF_FFFF)
        #expect(outbound.channel == 0)
        #expect(outbound.wantAck == false)
        #expect(outbound.hopLimit == nil)
    }

    @Test func outboundInitStoresEveryField() {
        let outbound = MeshtasticOutbound(portnum: 76, payload: Data([9, 8]), to: 0x1234, channel: 2,
                                          wantAck: true, hopLimit: 3)
        #expect(outbound.portnum == 76)
        #expect(outbound.payload == Data([9, 8]))
        #expect(outbound.to == 0x1234)
        #expect(outbound.channel == 2)
        #expect(outbound.wantAck == true)
        #expect(outbound.hopLimit == 3)
    }

    @Test func discoveredRadioInitStoresEveryField() {
        let id = UUID()
        let radio = DiscoveredRadio(id: id, name: "Meshtastic_1234", rssi: -61)
        #expect(radio.id == id)
        #expect(radio.name == "Meshtastic_1234")
        #expect(radio.rssi == -61)
    }

    @Test func meshGATTUUIDsMatchMeshtasticFirmware() {
        #expect(MeshGATT.serviceUUID.uuidString == "6BA1B218-15A8-461F-9FA8-5DCAE273EAFD")
        #expect(MeshGATT.toRadioUUID.uuidString == "F75C76D2-129E-4DAD-A1DD-7866124401E7")
        #expect(MeshGATT.fromRadioUUID.uuidString == "2C55E69E-4993-11ED-B878-0242AC120002")
        #expect(MeshGATT.fromNumUUID.uuidString == "ED9DA18C-A800-4F66-A670-AA7547E34453")
    }

    @Test func wirePortSeventySixDecodesAsReticulumTunnel() {
        // Pattern match, not `==`: PortNum is RawRepresentable, so `==` compares raw values and
        // would also accept `.UNRECOGNIZED(76)`.
        guard case .reticulumTunnelApp? = PortNum(rawValue: 76) else {
            Issue.record("rawValue 76 did not decode as .reticulumTunnelApp")
            return
        }
    }

    @Test func onlyUnauthorizedAndUnsupportedAreTerminal() {
        #expect(MeshtasticAdapterUnavailable.unauthorized.isTerminal)
        #expect(MeshtasticAdapterUnavailable.unsupported.isTerminal)
        #expect(!MeshtasticAdapterUnavailable.poweredOff.isTerminal)
        #expect(!MeshtasticAdapterUnavailable.other("x").isTerminal)
    }
}
