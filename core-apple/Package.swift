// swift-tools-version: 6.1
import PackageDescription

// AmbientLinkCore — the vendor-neutral Apple-platform core for Ambient Link:
// relay client + models, the GlassLink capture contract, and the backpressure
// primitives (EphemeralBuffer / Throttle) ported from the Cosmo teardown.
//
// Apple-vendor-specific code (Foundation Models "Siri AI", App Intents, the
// visionOS frontend) stays in ambient-link-apple's AmbientLinkKit, which depends
// on this package. ambient-link-meta/relay-ios consumes the same core.
let package = Package(
    name: "core-apple",
    platforms: [
        .visionOS(.v26),
        .iOS(.v26),
        .macOS(.v26),
        .watchOS(.v26),
    ],
    products: [
        .library(name: "AmbientLinkCore", targets: ["AmbientLinkCore"]),
    ],
    targets: [
        .target(name: "AmbientLinkCore"),
        .testTarget(
            name: "AmbientLinkCoreTests",
            dependencies: ["AmbientLinkCore"]
        ),
    ]
)
