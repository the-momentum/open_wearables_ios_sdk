import XCTest
@testable import OpenWearablesHealthSDK

/// Covers the flag that carries a sync blocked by a locked device over to a later wake.
/// The unlock notification only reaches a process that is still running, so the flag has
/// to outlive the process, and the next run must take it over.
final class LockedDeviceSyncTests: XCTestCase {

    private let suiteName = "com.openwearables.healthsdk.state"
    private let key = "pendingSyncAfterUnlock"

    func testDeferredSyncSurvivesAProcessRestart() {
        withIsolatedSDK { sdk, _ in
            let previous = sdk.pendingSyncAfterUnlock
            defer { sdk.pendingSyncAfterUnlock = previous }
            sdk.pendingSyncAfterUnlock = false

            sdk.deferSyncUntilUnlock()

            // A fresh `UserDefaults` for the same suite reads what a relaunched process sees.
            XCTAssertTrue(UserDefaults(suiteName: suiteName)?.bool(forKey: key) ?? false)
            XCTAssertTrue(sdk.pendingSyncAfterUnlock)
        }
    }

    func testTheNextSyncRunTakesOverTheDeferredSync() {
        withIsolatedSDK { sdk, _ in
            let previous = sdk.pendingSyncAfterUnlock
            let previousTypes = sdk.trackedTypes
            defer {
                sdk.pendingSyncAfterUnlock = previous
                sdk.trackedTypes = previousTypes
            }
            sdk.pendingSyncAfterUnlock = true
            sdk.trackedTypes = []

            var finished = false
            sdk.collectAllData(fullExport: false) { finished = true }

            XCTAssertTrue(finished, "with no tracked types the run ends before any HealthKit read")
            XCTAssertFalse(sdk.pendingSyncAfterUnlock)
        }
    }
}
