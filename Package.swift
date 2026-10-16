// swift-tools-version: 6.3

import PackageDescription

let package = Package(
    name: "MeshtasticKit",
    platforms: [.iOS(.v17), .macOS(.v13), .macCatalyst(.v17)],
    products: [
        .library(name: "MeshProto", targets: ["MeshProto"]),
        .library(name: "MeshtasticKit", targets: ["MeshtasticKit"]),
    ],
    dependencies: [
        .package(path: "../FCB"),
        .package(url: "https://github.com/apple/swift-protobuf", from: "1.28.0"),
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
        .testTarget(
            name: "MeshtasticKitTests",
            dependencies: ["MeshProto", "MeshtasticKit"]
        ),
    ],
    swiftLanguageModes: [.v6]
)
