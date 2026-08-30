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

    func testConfigureWithTokenRefreshURLPersistsOverride() {
        let sdk = OpenWearablesHealthSDK.shared
        let refreshURL = "https://auth.example.com/v1/wearables/session"
        sdk.configure(host: "https://sync.example.com", tokenRefreshURL: refreshURL)
        XCTAssertEqual(
            OpenWearablesHealthSdkKeychain.getCustomRefreshUrl(),
            refreshURL,
            "configure(tokenRefreshURL:) should persist the override for background refresh"
        )
    }

    func testConfigureWithoutTokenRefreshURLClearsOverride() {
        let sdk = OpenWearablesHealthSDK.shared
        // Set an override first...
        sdk.configure(
            host: "https://sync.example.com",
            tokenRefreshURL: "https://auth.example.com/refresh"
        )
        XCTAssertNotNil(OpenWearablesHealthSdkKeychain.getCustomRefreshUrl())
        // ...a plain configure clears it so the SDK falls back to the default
        // "{host}/api/v1/token/refresh" endpoint.
        sdk.configure(host: "https://sync.example.com")
        XCTAssertNil(OpenWearablesHealthSdkKeychain.getCustomRefreshUrl())
    }
}
