@preconcurrency import CoreBluetooth

/// Meshtastic BLE GATT UUIDs (current firmware).
public enum MeshGATT {
    /// Primary Meshtastic service.
    public static let serviceUUID = CBUUID(string: "6ba1b218-15a8-461f-9fa8-5dcae273eafd")
    /// Write: client → radio (serialized ToRadio protobuf).
    public static let toRadioUUID = CBUUID(string: "f75c76d2-129e-4dad-a1dd-7866124401e7")
    /// Read: radio → client (one serialized FromRadio protobuf per read; empty = drained).
    public static let fromRadioUUID = CBUUID(string: "2c55e69e-4993-11ed-b878-0242ac120002")
    /// Notify: radio signals new FromRadio data is available.
    public static let fromNumUUID = CBUUID(string: "ed9da18c-a800-4f66-a670-aa7547e34453")
}
