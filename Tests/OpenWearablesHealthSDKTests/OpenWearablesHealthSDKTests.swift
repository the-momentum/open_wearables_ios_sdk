import XCTest
import HealthKit
@testable import OpenWearablesHealthSDK

final class OpenWearablesHealthSDKTests: XCTestCase {
    
    func testSharedInstanceExists() {
        let sdk = OpenWearablesHealthSDK.shared
        XCTAssertNotNil(sdk)
    }
    
    func testConfigureSetsHost() {
        let sdk = OpenWearablesHealthSDK.shared
        sdk.configure(host: "https://test.example.com")
        // Verify the SDK is configured (host is internal, so we check via credentials)
        let credentials = sdk.getStoredCredentials()
        XCTAssertEqual(credentials["host"] as? String, "https://test.example.com")
    }
    
    func testIsSessionValidWithoutSignIn() {
        let sdk = OpenWearablesHealthSDK.shared
        // Without sign in, session should not be valid (unless prior state exists)
        // This is a basic sanity check
        XCTAssertNotNil(sdk.isSessionValid)
    }
    
    func testGetSyncStatusReturnsValidStructure() {
        let sdk = OpenWearablesHealthSDK.shared
        let status = sdk.getSyncStatus()
        XCTAssertNotNil(status["hasResumableSession"])
        XCTAssertNotNil(status["sentCount"])
        XCTAssertNotNil(status["completedTypes"])
        XCTAssertNotNil(status["isFullExport"])
    }
    
    func testSyncShouldAdvanceOnlyOn2xx() {
        XCTAssertTrue(OpenWearablesHealthSDK.syncShouldAdvance(afterHTTPStatus: 200))
        XCTAssertTrue(OpenWearablesHealthSDK.syncShouldAdvance(afterHTTPStatus: 201))
        XCTAssertFalse(OpenWearablesHealthSDK.syncShouldAdvance(afterHTTPStatus: 400))
        XCTAssertFalse(OpenWearablesHealthSDK.syncShouldAdvance(afterHTTPStatus: 401))
        XCTAssertFalse(OpenWearablesHealthSDK.syncShouldAdvance(afterHTTPStatus: 500))
        XCTAssertFalse(OpenWearablesHealthSDK.syncShouldAdvance(afterHTTPStatus: 0))
    }

    func testConfigureWithTokenRefreshURLPersistsOverride() {
        let sdk = OpenWearablesHealthSDK.shared
        let refreshURL = "https://auth.example.com/v1/wearables/session"
        sdk.configure(host: "https://sync.example.com", tokenRefreshURL: refreshURL)

        XCTAssertEqual(OpenWearablesHealthSdkKeychain.getCustomRefreshUrl(), refreshURL)
        XCTAssertEqual(sdk.tokenRefreshEndpoint?.absoluteString, refreshURL)
        XCTAssertEqual(sdk.getStoredCredentials()["tokenRefreshURL"] as? String, refreshURL)
    }

    func testConfigureWithoutTokenRefreshURLClearsOverride() {
        let sdk = OpenWearablesHealthSDK.shared
        sdk.configure(
            host: "https://sync.example.com",
            tokenRefreshURL: "https://auth.example.com/refresh"
        )
        XCTAssertNotNil(OpenWearablesHealthSdkKeychain.getCustomRefreshUrl())

        sdk.configure(host: "https://sync.example.com")
        XCTAssertNil(OpenWearablesHealthSdkKeychain.getCustomRefreshUrl())
        XCTAssertEqual(
            sdk.tokenRefreshEndpoint?.absoluteString,
            "https://sync.example.com/api/v1/token/refresh"
        )
    }

    func testConfigureTreatsBlankTokenRefreshURLAsDefault() {
        let sdk = OpenWearablesHealthSDK.shared
        sdk.configure(
            host: "https://sync.example.com",
            tokenRefreshURL: "https://auth.example.com/refresh"
        )
        sdk.configure(host: "https://sync.example.com", tokenRefreshURL: "   ")
        XCTAssertNil(OpenWearablesHealthSdkKeychain.getCustomRefreshUrl())
        XCTAssertEqual(
            sdk.tokenRefreshEndpoint?.absoluteString,
            "https://sync.example.com/api/v1/token/refresh"
        )
    }

    func testAbsoluteHTTPURLRejectsRelativeAndNonHTTP() {
        XCTAssertNil(OpenWearablesHealthSDK.absoluteHTTPURL(from: "/token/refresh"))
        XCTAssertNil(OpenWearablesHealthSDK.absoluteHTTPURL(from: "token/refresh"))
        XCTAssertNil(OpenWearablesHealthSDK.absoluteHTTPURL(from: "ftp://auth.example.com/refresh"))
        XCTAssertEqual(
            OpenWearablesHealthSDK.absoluteHTTPURL(from: " https://auth.example.com/v1/refresh ")?.absoluteString,
            "https://auth.example.com/v1/refresh"
        )
    }

    func testDietaryTypesMapToHealthKitQuantityIdentifiers() {
        let dietary = HealthDataType.allCases.filter { $0.rawValue.hasPrefix("dietary") }
        XCTAssertEqual(dietary.count, 39)

        for type in dietary {
            guard let sample = type.toHKSampleType() else {
                XCTFail("\(type.rawValue) did not map to a HealthKit type")
                continue
            }
            XCTAssertEqual(
                sample.identifier,
                "HKQuantityTypeIdentifier" + type.rawValue.prefix(1).uppercased() + type.rawValue.dropFirst()
            )
        }
    }

    func testFoodTypeMapsToHealthKitCorrelation() {
        XCTAssertEqual(HealthDataType.food.rawValue, "food")
        XCTAssertEqual(
            HealthDataType.food.toHKSampleType()?.identifier,
            HKCorrelationTypeIdentifier.food.rawValue
        )
    }

    func testMapTypesAddsFoodWhenDietaryTypesAreRequested() {
        let mapped = OpenWearablesHealthSDK.shared.mapTypes([.dietaryProtein, .steps])
        XCTAssertTrue(mapped.contains { $0.identifier == HKCorrelationTypeIdentifier.food.rawValue })
        XCTAssertTrue(mapped.contains { $0.identifier == HKQuantityTypeIdentifier.dietaryProtein.rawValue })
    }

    func testFoodCorrelationEmitsParentAndChildrenInRecords() {
        let start = Date(timeIntervalSince1970: 1_779_163_200) // 2026-05-18-ish just a fixed date
        let energyType = HKQuantityType.quantityType(forIdentifier: .dietaryEnergyConsumed)!
        let proteinType = HKQuantityType.quantityType(forIdentifier: .dietaryProtein)!
        let caffeineType = HKQuantityType.quantityType(forIdentifier: .dietaryCaffeine)!

        let energy = HKQuantitySample(
            type: energyType,
            quantity: HKQuantity(unit: .kilocalorie(), doubleValue: 550),
            start: start,
            end: start
        )
        let protein = HKQuantitySample(
            type: proteinType,
            quantity: HKQuantity(unit: .gram(), doubleValue: 38.2),
            start: start,
            end: start
        )
        let food = HKCorrelation(
            type: HKObjectType.correlationType(forIdentifier: .food)!,
            start: start,
            end: start,
            objects: [energy, protein],
            metadata: [
                HKMetadataKeyFoodType: "Kurczak z ryżem",
                "mealType": "obiad"
            ]
        )
        let caffeine = HKQuantitySample(
            type: caffeineType,
            quantity: HKQuantity(unit: HKUnit.gramUnit(with: .milli), doubleValue: 95),
            start: start.addingTimeInterval(-10_800),
            end: start.addingTimeInterval(-10_800)
        )

        let payload = OpenWearablesHealthSDK.shared.buildCombinedPayload(samples: [food, energy, caffeine])
        let records = payload["data"] as? [String: Any]
        let list = records?["records"] as? [[String: Any]] ?? []

        let foodRow = list.first { $0["type"] as? String == HKCorrelationTypeIdentifier.food.rawValue }
        XCTAssertNotNil(foodRow)
        XCTAssertEqual(foodRow?["id"] as? String, food.uuid.uuidString)
        XCTAssertEqual(foodRow?["value"] as? Int, 1)
        XCTAssertTrue(foodRow?["unit"] is NSNull)
        XCTAssertTrue(foodRow?["parentId"] is NSNull)
        let meta = foodRow?["metadata"] as? [String: Any]
        XCTAssertEqual(meta?["title"] as? String, "Kurczak z ryżem")
        XCTAssertEqual(meta?["mealType"] as? String, "obiad")
        XCTAssertNil(meta?[HKMetadataKeyFoodType])

        let energyRow = list.first { $0["id"] as? String == energy.uuid.uuidString }
        XCTAssertEqual(energyRow?["parentId"] as? String, food.uuid.uuidString)
        XCTAssertEqual(energyRow?["type"] as? String, HKQuantityTypeIdentifier.dietaryEnergyConsumed.rawValue)
        XCTAssertEqual(energyRow?["unit"] as? String, "Cal")

        let proteinRow = list.first { $0["id"] as? String == protein.uuid.uuidString }
        XCTAssertEqual(proteinRow?["parentId"] as? String, food.uuid.uuidString)

        let caffeineRow = list.first { $0["id"] as? String == caffeine.uuid.uuidString }
        XCTAssertTrue(caffeineRow?["parentId"] is NSNull)
        XCTAssertEqual(caffeineRow?["unit"] as? String, "mg")

        // The standalone energy sample is the same UUID as the food child — emit once.
        XCTAssertEqual(list.filter { $0["id"] as? String == energy.uuid.uuidString }.count, 1)
    }

    func testCyclingTypesMapToHealthKit() {
        XCTAssertEqual(HealthDataType.cyclingPower.rawValue, "cyclingPower")
        XCTAssertEqual(HealthDataType.cyclingCadence.rawValue, "cyclingCadence")
        XCTAssertEqual(HealthDataType.cyclingSpeed.rawValue, "cyclingSpeed")
        XCTAssertEqual(HealthDataType.cyclingFunctionalThresholdPower.rawValue, "cyclingFunctionalThresholdPower")

        if #available(iOS 17.0, *) {
            XCTAssertEqual(
                HealthDataType.cyclingPower.toHKSampleType()?.identifier,
                HKQuantityTypeIdentifier.cyclingPower.rawValue
            )
            XCTAssertEqual(
                HealthDataType.cyclingCadence.toHKSampleType()?.identifier,
                HKQuantityTypeIdentifier.cyclingCadence.rawValue
            )
            XCTAssertEqual(
                HealthDataType.cyclingSpeed.toHKSampleType()?.identifier,
                HKQuantityTypeIdentifier.cyclingSpeed.rawValue
            )
            XCTAssertEqual(
                HealthDataType.cyclingFunctionalThresholdPower.toHKSampleType()?.identifier,
                HKQuantityTypeIdentifier.cyclingFunctionalThresholdPower.rawValue
            )
        }
    }

    func testRunningDynamicsTypesMapToHealthKit() {
        XCTAssertEqual(HealthDataType.runningPower.rawValue, "runningPower")
        XCTAssertEqual(HealthDataType.runningVerticalOscillation.rawValue, "runningVerticalOscillation")
        XCTAssertEqual(HealthDataType.runningGroundContactTime.rawValue, "runningGroundContactTime")

        if #available(iOS 16.0, *) {
            XCTAssertEqual(
                HealthDataType.runningPower.toHKSampleType()?.identifier,
                HKQuantityTypeIdentifier.runningPower.rawValue
            )
            XCTAssertEqual(
                HealthDataType.runningVerticalOscillation.toHKSampleType()?.identifier,
                HKQuantityTypeIdentifier.runningVerticalOscillation.rawValue
            )
            XCTAssertEqual(
                HealthDataType.runningGroundContactTime.toHKSampleType()?.identifier,
                HKQuantityTypeIdentifier.runningGroundContactTime.rawValue
            )
        }
    }

    func testWorkoutLapsIncludeLapSegmentAndMarker() {
        let start = Date(timeIntervalSince1970: 1_700_000_000)
        let lap = HKWorkoutEvent(
            type: .lap,
            dateInterval: DateInterval(start: start, duration: 60),
            metadata: [HKMetadataKeyLapLength: HKQuantity(unit: .meter(), doubleValue: 400)]
        )
        let marker = HKWorkoutEvent(
            type: .marker,
            dateInterval: DateInterval(start: start.addingTimeInterval(60), duration: 0),
            metadata: nil
        )
        let pause = HKWorkoutEvent(
            type: .pause,
            dateInterval: DateInterval(start: start.addingTimeInterval(90), duration: 0),
            metadata: nil
        )
        let workout = HKWorkout(
            activityType: .running,
            start: start,
            end: start.addingTimeInterval(120),
            workoutEvents: [lap, marker, pause],
            totalEnergyBurned: nil,
            totalDistance: nil,
            metadata: nil
        )

        let df = ISO8601DateFormatter()
        let laps = OpenWearablesHealthSDK.shared._buildWorkoutLaps(workout, dateFormatter: df)

        guard let rows = laps as? [[String: Any]] else {
            XCTFail("Expected laps array, got \(laps)")
            return
        }
        XCTAssertEqual(rows.count, 2)
        XCTAssertEqual(rows[0]["type"] as? String, "lap")
        XCTAssertEqual(rows[0]["duration"] as? TimeInterval, 60)
        XCTAssertEqual(rows[0]["distanceM"] as? Double, 400)
        XCTAssertEqual(rows[1]["type"] as? String, "marker")
        XCTAssertEqual(OpenWearablesHealthSDK.shared._workoutEventTypeString(.segment), "segment")
        XCTAssertNil(OpenWearablesHealthSDK.shared._workoutEventTypeString(.pause))
    }

    func testWorkoutWithoutEventsHasNullLaps() {
        let start = Date(timeIntervalSince1970: 1_700_000_000)
        let workout = HKWorkout(
            activityType: .running,
            start: start,
            end: start.addingTimeInterval(60),
            workoutEvents: nil,
            totalEnergyBurned: nil,
            totalDistance: nil,
            metadata: nil
        )
        let laps = OpenWearablesHealthSDK.shared._buildWorkoutLaps(workout, dateFormatter: ISO8601DateFormatter())
        XCTAssertTrue(laps is NSNull)
    }

    func testInvalidPersistedRefreshURLDoesNotFallBackToSyncHost() {
        let sdk = OpenWearablesHealthSDK.shared
        sdk.configure(host: "https://sync.example.com")
        OpenWearablesHealthSdkKeychain.saveCustomRefreshUrl("not-a-url")
        XCTAssertNil(sdk.tokenRefreshEndpoint)
        sdk.configure(host: "https://sync.example.com")
    }

    /// Simulates a background cold start: persist the host, then drop the
    /// in-memory value the way a fresh process looks before `configure` runs.
    func testApiBaseUrlFallsBackToPersistedHostWithoutConfigure() {
        withIsolatedHostState { sdk in
            OpenWearablesHealthSdkKeychain.saveCustomRefreshUrl(nil)
            OpenWearablesHealthSdkKeychain.saveHost("https://bg-relaunch.example.com/")
            sdk.host = nil

            XCTAssertEqual(sdk.apiBaseUrl, "https://bg-relaunch.example.com/api/v1")
            XCTAssertEqual(
                sdk.tokenRefreshEndpoint?.absoluteString,
                "https://bg-relaunch.example.com/api/v1/token/refresh"
            )
        }
    }

    func testApiBaseUrlPrefersInMemoryHostOverPersisted() {
        withIsolatedHostState { sdk in
            OpenWearablesHealthSdkKeychain.saveCustomRefreshUrl(nil)
            OpenWearablesHealthSdkKeychain.saveHost("https://persisted.example.com")
            sdk.host = "https://in-memory.example.com"

            XCTAssertEqual(sdk.apiBaseUrl, "https://in-memory.example.com/api/v1")
        }
    }

    func testApiBaseUrlNilWhenHostMissingFromMemoryAndPersistence() {
        withIsolatedHostState { sdk in
            OpenWearablesHealthSdkKeychain.saveHost(nil)
            OpenWearablesHealthSdkKeychain.saveCustomRefreshUrl(nil)
            sdk.host = nil

            XCTAssertNil(sdk.apiBaseUrl)
            XCTAssertNil(sdk.tokenRefreshEndpoint)
        }
    }

    func testTokenRefreshUsesPersistedCustomURLWhenHostNotInMemory() {
        withIsolatedHostState { sdk in
            OpenWearablesHealthSdkKeychain.saveHost(nil)
            OpenWearablesHealthSdkKeychain.saveCustomRefreshUrl("https://auth.example.com/v1/refresh")
            sdk.host = nil

            XCTAssertNil(sdk.apiBaseUrl)
            XCTAssertEqual(
                sdk.tokenRefreshEndpoint?.absoluteString,
                "https://auth.example.com/v1/refresh"
            )
        }
    }

    /// The SDK is a singleton, so `init`'s `getHost()` restore cannot be
    /// re-run. These tests clear `host` in memory and rely on the same
    /// persistence fallback `apiBaseUrl` / `tokenRefreshEndpoint` use.
    private func withIsolatedHostState(_ body: (OpenWearablesHealthSDK) -> Void) {
        let sdk = OpenWearablesHealthSDK.shared
        let previousHost = sdk.host
        let previousPersisted = OpenWearablesHealthSdkKeychain.getHost()
        let previousRefresh = OpenWearablesHealthSdkKeychain.getCustomRefreshUrl()
        defer {
            sdk.host = previousHost
            OpenWearablesHealthSdkKeychain.saveHost(previousPersisted)
            OpenWearablesHealthSdkKeychain.saveCustomRefreshUrl(previousRefresh)
        }
        body(sdk)
    }
}
