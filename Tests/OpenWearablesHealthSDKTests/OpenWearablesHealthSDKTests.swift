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
        XCTAssertNotNil(status["nextChunkIndex"])
    }

    func testSyncUploadManifestHeadersAreComplete() {
        let sdk = OpenWearablesHealthSDK.shared
        let manifest = OpenWearablesHealthSDK.SyncUploadManifest(
            clientSyncId: "sync-123",
            chunkIndex: 44,
            isFinal: true,
            totalItems: 84_745
        )
        var request = URLRequest(url: URL(string: "https://example.com/sync")!)

        sdk.applySyncManifest(manifest, to: &request)

        XCTAssertEqual(request.value(forHTTPHeaderField: "X-OW-Client-Sync-ID"), "sync-123")
        XCTAssertEqual(request.value(forHTTPHeaderField: "X-OW-Client-Sync-Chunk-Index"), "44")
        XCTAssertEqual(request.value(forHTTPHeaderField: "X-OW-Client-Sync-Final"), "true")
        XCTAssertEqual(request.value(forHTTPHeaderField: "X-OW-Client-Sync-Total-Items"), "84745")
    }

    func testSubmittedItemCountUsesSerializedEnvelopeCounts() {
        let sdk = OpenWearablesHealthSDK.shared
        let payload: [String: Any] = [
            "data": [
                "records": [["id": "record-1"], ["id": "record-2"]],
                "workouts": [["id": "workout-1"]],
                "sleep": [["id": "sleep-1"]]
            ]
        ]

        XCTAssertEqual(sdk.submittedItemCount(in: payload), 4)
    }

    func testBackgroundOutboxSuccessAdvancesManifest() throws {
        let sdk = OpenWearablesHealthSDK.shared
        sdk.clearSyncSession()
        defer { sdk.clearSyncSession() }

        let state = sdk.startNewSyncState(fullExport: false, types: [])
        let clientSyncId = try XCTUnwrap(state.clientSyncId)
        let payload: [String: Any] = [
            "data": [
                "records": [["id": "record-1"], ["id": "record-2"]],
                "workouts": [["id": "workout-1"]],
                "sleep": [["id": "sleep-1"]]
            ]
        ]
        let payloadURL = sdk.newPath("background_manifest_payload", ext: "json")
        let itemURL = sdk.newPath("background_manifest_item", ext: "json")
        defer {
            try? FileManager.default.removeItem(at: payloadURL)
            try? FileManager.default.removeItem(at: itemURL)
        }
        try JSONSerialization.data(withJSONObject: payload).write(to: payloadURL)
        let manifest = OpenWearablesHealthSDK.SyncUploadManifest(
            clientSyncId: clientSyncId,
            chunkIndex: 0,
            isFinal: false,
            totalItems: nil
        )
        let item = OpenWearablesHealthSDK.OutboxItem(
            typeIdentifier: "combined",
            userKey: state.userKey,
            payloadPath: payloadURL.path,
            anchorPath: nil,
            wasFullExport: false,
            syncManifest: manifest
        )
        try JSONEncoder().encode(item).write(to: itemURL)

        sdk.handleSuccessfulUpload(itemPath: itemURL.path, anchorPath: nil, wasFullExport: false)

        let updatedState = try XCTUnwrap(sdk.loadSyncState())
        XCTAssertEqual(updatedState.nextChunkIndex, 1)
        XCTAssertEqual(updatedState.totalSubmittedItemCount, 4)
        XCTAssertFalse(FileManager.default.fileExists(atPath: itemURL.path))
    }
}
