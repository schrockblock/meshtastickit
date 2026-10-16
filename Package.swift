// swift-tools-version: 6.3

import PackageDescription

let package = Package(
    name: "MeshtasticKit",
    platforms: [.iOS(.v17), .macOS(.v13), .macCatalyst(.v17)],
    products: [
        .library(name: "MeshProto", targets: ["MeshProto"]),
        .library(name: "MeshtasticKit", targets: ["MeshtasticKit"]),
        .library(name: "MeshtasticMock", targets: ["MeshtasticMock"]),
    ],
    dependencies: [
        .package(path: "../FCB"),
        .package(url: "https://github.com/apple/swift-protobuf", from: "1.28.0"),
        .package(url: "https://github.com/NordicSemiconductor/IOS-CoreBluetooth-Mock.git", from: "1.0.0"),
        // Test-only (ImmediateClock, TestClock, LockIsolated). Already in the graph via FCB.
        .package(url: "https://github.com/pointfreeco/swift-composable-architecture", from: "1.0.0"),
    ],
    targets: [
        .target(
            name: "MeshProto",
            dependencies: [.product(name: "SwiftProtobuf", package: "swift-protobuf")]
        ),
        .target(
            name: "MeshtasticKit",
            dependencies: [
                "MeshProto",
                .product(name: "FCBComposable", package: "FCB"),
            ]
        ),
        .target(
            name: "MeshtasticMock",
            dependencies: [
                "MeshProto",
                "MeshtasticKit",
                .product(name: "CoreBluetoothMock", package: "IOS-CoreBluetooth-Mock"),
            ]
        ),
        .testTarget(
            name: "MeshtasticKitTests",
            dependencies: [
                "MeshProto",
                "MeshtasticKit",
                "MeshtasticMock",
                .product(name: "FCBComposable", package: "FCB"),
                .product(name: "CoreBluetoothMock", package: "IOS-CoreBluetooth-Mock"),
                .product(name: "ComposableArchitecture", package: "swift-composable-architecture"),
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
