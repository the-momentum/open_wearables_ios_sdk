import XCTest
import HealthKit
@testable import OpenWearablesHealthSDK

/// Food and water items deleted in HealthKit are sent by id and type, so the backend can remove them. An edited
/// item comes back under a new id, so without its deletion the old one would stay and the day would overcount.
final class DeletionPayloadTests: XCTestCase {

    private let energy = HKQuantityType.quantityType(forIdentifier: .dietaryEnergyConsumed)!

    private func data(_ payload: [String: Any]) -> [String: Any] {
        payload["data"] as? [String: Any] ?? [:]
    }

    func testOnlyFoodAndWaterTypesSendDeletions() {
        XCTAssertTrue(OpenWearablesHealthSDK.sendsDeletions(of: energy))
        XCTAssertTrue(OpenWearablesHealthSDK.sendsDeletions(of: HKQuantityType.quantityType(forIdentifier: .dietaryWater)!))
        XCTAssertFalse(OpenWearablesHealthSDK.sendsDeletions(of: HKQuantityType.quantityType(forIdentifier: .heartRate)!))
        XCTAssertFalse(OpenWearablesHealthSDK.sendsDeletions(of: HKQuantityType.quantityType(forIdentifier: .stepCount)!))
        XCTAssertFalse(OpenWearablesHealthSDK.sendsDeletions(of: HKObjectType.correlationType(forIdentifier: .food)!))
    }

    func testDeletedItemsAreSentWithTheirType() {
        let id = UUID()
        let payload = OpenWearablesHealthSDK.shared.buildCombinedPayload(
            samples: [], deletions: OpenWearablesHealthSDK.deletionEntries(type: energy, ids: [id])
        )

        let deletions = data(payload)["deletions"] as? [[String: String]]
        XCTAssertEqual(deletions ?? [], [["id": id.uuidString, "type": energy.identifier]])
    }

    func testPayloadWithoutDeletionsLeavesTheKeyOut() {
        let payload = OpenWearablesHealthSDK.shared.buildCombinedPayload(samples: [])

        XCTAssertNil(data(payload)["deletions"])
    }

    func testAChunkWithOnlyDeletionsHasAPayload() {
        let onlyDeletions = OpenWearablesHealthSDK.TypeRoundResult(
            type: energy, samples: [], count: 0, nextOlderThan: nil, newAnchor: nil, anchorData: nil, isDone: true,
            deletedIds: [UUID()]
        )
        let empty = OpenWearablesHealthSDK.TypeRoundResult(
            type: energy, samples: [], count: 0, nextOlderThan: nil, newAnchor: nil, anchorData: nil, isDone: true
        )

        XCTAssertTrue(onlyDeletions.hasPayload)
        XCTAssertFalse(empty.hasPayload)
    }
}
