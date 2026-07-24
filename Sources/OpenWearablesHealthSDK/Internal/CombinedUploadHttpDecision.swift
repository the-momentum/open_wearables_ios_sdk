import Foundation

/// How a combined-upload HTTP status should affect sync progress.
///
/// Client errors other than 401 must not be treated as success: doing so lets
/// the orchestrator advance HealthKit anchors and skip the rejected samples
/// forever (see the-momentum/open_wearables_ios_sdk#20).
internal enum CombinedUploadHttpDecision: Equatable {
    /// 2xx — persist progress / anchors and continue.
    case success
    /// 401 — refresh credentials and retry the same payload once.
    case refreshAndRetry
    /// Any other status — do not advance anchors; fail this sync so a later
    /// wake can rebuild the payload from HealthKit.
    case failWithoutAdvance
}

internal enum CombinedUploadHttp {
    static func decision(forStatusCode statusCode: Int) -> CombinedUploadHttpDecision {
        if (200...299).contains(statusCode) {
            return .success
        }
        if statusCode == 401 {
            return .refreshAndRetry
        }
        return .failWithoutAdvance
    }
}
