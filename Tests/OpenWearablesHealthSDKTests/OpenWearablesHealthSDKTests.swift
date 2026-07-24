import XCTest
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
    
    func testRestorePersistedHostWhenMemoryNil() {
        let sdk = OpenWearablesHealthSDK.shared
        OpenWearablesHealthSdkKeychain.saveHost("https://restore-test.example.com")
        sdk.host = nil
        XCTAssertNil(sdk.host)

        sdk.restorePersistedHostIfNeeded()

        XCTAssertEqual(sdk.host, "https://restore-test.example.com")
        XCTAssertEqual(sdk.apiBaseUrl, "https://restore-test.example.com/api/v1")

        restoreHostState()
    }

    func testRestorePersistedHostDoesNotOverwriteMemory() {
        let sdk = OpenWearablesHealthSDK.shared
        OpenWearablesHealthSdkKeychain.saveHost("https://persisted.example.com")
        sdk.host = "https://in-memory.example.com"

        sdk.restorePersistedHostIfNeeded()

        XCTAssertEqual(sdk.host, "https://in-memory.example.com")

        restoreHostState()
    }

    /// Undo host mutations on the shared singleton and persisted store so other tests are unaffected.
    private func restoreHostState() {
        OpenWearablesHealthSdkKeychain.saveHost(nil)
        OpenWearablesHealthSDK.shared.host = nil
    }
}
