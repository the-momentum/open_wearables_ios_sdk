import XCTest
import HealthKit
@testable import OpenWearablesHealthSDK

/// `HKUnit.percent()` is a 0-1 fraction, but the payload labels it "%", which the backend
/// reads as 0-100 (the same scale the Android SDK sends).
final class PercentageScaleTests: XCTestCase {

    private func record(_ identifier: HKQuantityTypeIdentifier, fraction: Double) -> [String: Any]? {
        let sample = HKQuantitySample(
            type: HKQuantityType.quantityType(forIdentifier: identifier)!,
            quantity: HKQuantity(unit: .percent(), doubleValue: fraction),
            start: Date(),
            end: Date()
        )
        let payload = OpenWearablesHealthSDK.shared.buildCombinedPayload(samples: [sample])
        let data = payload["data"] as? [String: Any]
        return (data?["records"] as? [[String: Any]])?.first
    }

    func testPercentTypesAreSentOnA0To100Scale() {
        let cases: [(HKQuantityTypeIdentifier, Double, Double)] = [
            (.bodyFatPercentage, 0.304, 30.4),
            (.walkingAsymmetryPercentage, 0.12, 12),
            (.walkingDoubleSupportPercentage, 0.285, 28.5),
            (.oxygenSaturation, 0.97, 97),
        ]

        for (identifier, fraction, expected) in cases {
            let r = record(identifier, fraction: fraction)
            XCTAssertEqual(r?["unit"] as? String, "%", identifier.rawValue)
            XCTAssertEqual(r?["value"] as? Double ?? .nan, expected, accuracy: 1e-9, identifier.rawValue)
        }
    }
}
