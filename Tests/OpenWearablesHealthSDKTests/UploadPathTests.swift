import XCTest
@testable import OpenWearablesHealthSDK

/// Covers the combined-upload path: which HTTP outcomes are allowed to advance sync
/// progress, and how a 401 mid-round is recovered. See issue #34.
final class UploadPathTests: XCTestCase {

    private let payload: [String: Any] = [
        "provider": "apple",
        "data": ["records": [], "sleep": [], "workouts": []]
    ]

    /// Drives one upload inside a live sync generation and returns what it reported.
    /// `nil` means the completion never fired.
    private func upload(
        on sdk: OpenWearablesHealthSDK,
        credential: String = "access-1",
        syncContext: SyncRequestContext? = nil,
        file: StaticString = #filePath,
        line: UInt = #line
    ) -> Bool? {
        guard let endpoint = sdk.syncEndpoint else {
            XCTFail("No sync endpoint", file: file, line: line)
            return nil
        }
        guard let generation = sdk.beginSyncRun() else {
            XCTFail("Could not claim the sync slot", file: file, line: line)
            return nil
        }
        defer { sdk.finishSync(generation: generation) }

        var outcome: Bool?
        sdk.uploadCombinedPayload(
            payload: payload, endpoint: endpoint, credential: credential, generation: generation,
            syncContext: syncContext
        ) { outcome = $0 }

        waitUntil { outcome != nil }
        return outcome
    }

    func testUploadAdvancesOnSuccess() {
        withIsolatedSDK { sdk, _ in
            StubURLProtocol.install { _ in .status(200) }
            XCTAssertEqual(upload(on: sdk), true)
        }
    }

    /// The production `ClientDisconnect` 400: the server never stored the body, so the
    /// round has to be rebuilt rather than skipped.
    func testUploadDoesNotAdvanceOnClientError() {
        withIsolatedSDK { sdk, _ in
            StubURLProtocol.install { _ in .status(400, #"{"detail":"ClientDisconnect"}"#) }
            XCTAssertEqual(upload(on: sdk), false)
        }
    }

    func testUploadDoesNotAdvanceOnServerError() {
        withIsolatedSDK { sdk, _ in
            StubURLProtocol.install { _ in .status(500) }
            XCTAssertEqual(upload(on: sdk), false)
        }
    }

    func testUploadDoesNotAdvanceOnTransportError() {
        withIsolatedSDK { sdk, _ in
            StubURLProtocol.install { _ in .failure(URLError(.networkConnectionLost)) }
            XCTAssertEqual(upload(on: sdk), false)
        }
    }

    /// A 401 has to refresh once and replay the same chunk with the new token, instead
    /// of losing the round or signing the user out.
    func testUploadRetriesWithRefreshedTokenAfterUnauthorized() {
        withIsolatedSDK { sdk, _ in
            StubURLProtocol.install { request in
                if request.url?.path.hasSuffix("/token/refresh") == true {
                    return .status(200, #"{"access_token":"access-2","refresh_token":"refresh-2"}"#)
                }
                let credential = request.value(forHTTPHeaderField: "Authorization")
                return credential == "Bearer access-2" ? .status(200) : .status(401)
            }

            XCTAssertEqual(upload(on: sdk), true)

            let uploads = StubURLProtocol.requests(matching: "/sync")
            XCTAssertEqual(uploads.count, 2, "expected the original attempt plus one retry")
            XCTAssertEqual(uploads.first?.value(forHTTPHeaderField: "Authorization"), "Bearer access-1")
            XCTAssertEqual(uploads.last?.value(forHTTPHeaderField: "Authorization"), "Bearer access-2")
            XCTAssertEqual(
                uploads.first?.value(forHTTPHeaderField: "X-Request-Id"),
                uploads.last?.value(forHTTPHeaderField: "X-Request-Id"),
                "both attempts should carry one request id so the server can correlate them"
            )
            XCTAssertEqual(OpenWearablesHealthSdkKeychain.getAccessToken(), "access-2")
        }
    }

    /// App state and trigger travel as headers so server logs can split syncs by them.
    /// The 401 replay has to carry the same values as the attempt it repeats.
    func testUploadSendsSyncContextHeadersOnBothAttempts() {
        withIsolatedSDK { sdk, _ in
            StubURLProtocol.install { request in
                if request.url?.path.hasSuffix("/token/refresh") == true {
                    return .status(200, #"{"access_token":"access-2","refresh_token":"refresh-2"}"#)
                }
                let credential = request.value(forHTTPHeaderField: "Authorization")
                return credential == "Bearer access-2" ? .status(200) : .status(401)
            }

            let context = SyncRequestContext(inBackground: true, trigger: .healthKitObserver)
            XCTAssertEqual(upload(on: sdk, syncContext: context), true)

            let uploads = StubURLProtocol.requests(matching: "/sync")
            XCTAssertEqual(uploads.count, 2)
            for upload in uploads {
                XCTAssertEqual(upload.value(forHTTPHeaderField: "X-Open-Wearables-App-State"), "background")
                XCTAssertEqual(upload.value(forHTTPHeaderField: "X-Open-Wearables-Sync-Trigger"), "healthkit_observer")
            }
        }
    }

    func testUploadWithoutSyncContextOmitsItsHeaders() {
        withIsolatedSDK { sdk, _ in
            StubURLProtocol.install { _ in .status(200) }
            XCTAssertEqual(upload(on: sdk), true)

            let upload = StubURLProtocol.requests(matching: "/sync").first
            XCTAssertNil(upload?.value(forHTTPHeaderField: "X-Open-Wearables-App-State"))
            XCTAssertNil(upload?.value(forHTTPHeaderField: "X-Open-Wearables-Sync-Trigger"))
        }
    }

    /// A refresh token the server rejects is a genuine auth failure and has to surface
    /// to the host app rather than being retried forever.
    func testUploadReportsAuthErrorWhenRefreshIsRejected() {
        withIsolatedSDK { sdk, _ in
            var reportedStatus: Int?
            sdk.onAuthError = { status, _ in reportedStatus = status }

            StubURLProtocol.install { _ in .status(401) }

            XCTAssertEqual(upload(on: sdk), false)
            waitUntil { reportedStatus != nil }
            XCTAssertEqual(reportedStatus, 401)
        }
    }

    /// A round cancelled while its upload was in flight must not report success: the
    /// caller would advance cursors past data the next run still has to send.
    func testUploadDoesNotAdvanceWhenItsRunWasCancelled() {
        withIsolatedSDK { sdk, _ in
            guard let endpoint = sdk.syncEndpoint, let generation = sdk.beginSyncRun() else {
                return XCTFail("Could not start a sync run")
            }
            defer { sdk.finishSync(generation: generation) }

            StubURLProtocol.install { _ in .status(200) }
            sdk.cancelSync()

            var outcome: Bool?
            sdk.uploadCombinedPayload(
                payload: payload, endpoint: endpoint, credential: "access-1", generation: generation
            ) { outcome = $0 }

            waitUntil { outcome != nil }
            XCTAssertEqual(outcome, false)
        }
    }
}
