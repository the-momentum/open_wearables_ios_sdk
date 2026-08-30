import Foundation
import UIKit
import HealthKit

extension OpenWearablesHealthSDK {

    // MARK: - Logs Endpoint

    internal var logsEndpoint: URL? {
        guard let userId = userId, let base = apiBaseUrl else { return nil }
        return URL(string: "\(base)/sdk/users/\(userId)/logs")
    }

    // MARK: - Device State Collection

    internal func collectDeviceStateEvent() -> [String: Any] {
        let device = UIDevice.current
        device.isBatteryMonitoringEnabled = true

        let thermalState: String
        switch ProcessInfo.processInfo.thermalState {
        case .nominal: thermalState = "nominal"
        case .fair: thermalState = "fair"
        case .serious: thermalState = "serious"
        case .critical: thermalState = "critical"
        @unknown default: thermalState = "unknown"
        }

        let batteryState: String
        switch device.batteryState {
        case .unknown: batteryState = "unknown"
        case .unplugged: batteryState = "unplugged"
        case .charging: batteryState = "charging"
        case .full: batteryState = "full"
        @unknown default: batteryState = "unknown"
        }

        let totalRam = ProcessInfo.processInfo.physicalMemory
        let availableRam = os_proc_available_memory()

        let bgTimeRemaining = UIApplication.shared.backgroundTimeRemaining
        let taskType = bgTimeRemaining == .greatestFiniteMagnitude ? "foreground" : "background"

        return [
            "eventType": "device_state",
            "timestamp": ISO8601DateFormatter().string(from: Date()),
            "batteryLevel": device.batteryLevel,
            "batteryState": batteryState,
            "isLowPowerMode": ProcessInfo.processInfo.isLowPowerModeEnabled,
            "thermalState": thermalState,
            "taskType": taskType,
            "availableRamBytes": availableRam,
            "totalRamBytes": totalRam
        ]
    }

    // MARK: - Sync Start Log

    internal func sendSyncStartLog(
        types: [HKSampleType],
        typeCounts: [String: Int]?,
        startDate: Date?,
        endDate: Date,
        completion: @escaping () -> Void
    ) {
        guard let endpoint = logsEndpoint, let credential = authCredential else {
            completion()
            return
        }

        let formatter = ISO8601DateFormatter()

        let dataTypeCounts: [[String: Any]] = types.map { type in
            var item: [String: Any] = ["type": type.identifier]
            if let count = typeCounts?[type.identifier] {
                item["count"] = count
            }
            return item
        }

        var timeRange: [String: String] = ["endDate": formatter.string(from: endDate)]
        if let startDate = startDate {
            timeRange["startDate"] = formatter.string(from: startDate)
        }

        let startEvent: [String: Any] = [
            "eventType": "historical_data_sync_start",
            "timestamp": formatter.string(from: Date()),
            "dataTypeCounts": dataTypeCounts,
            "timeRange": timeRange
        ]

        let body: [String: Any] = [
            "sdkVersion": OpenWearablesHealthSDK.sdkVersion,
            "provider": "apple",
            "events": [startEvent, collectDeviceStateEvent()]
        ]

        sendLogRequest(endpoint: endpoint, credential: credential, body: body, completion: completion)
    }

    // MARK: - Per-Type End Log (fire-and-forget, called during sync as each type completes)

    internal func sendTypeEndLog(type: String, success: Bool, recordCount: Int, durationMs: Int) {
        guard let endpoint = logsEndpoint, let credential = authCredential else { return }

        let endEvent: [String: Any] = [
            "eventType": "historical_data_type_sync_end",
            "timestamp": ISO8601DateFormatter().string(from: Date()),
            "dataType": type,
            "success": success,
            "recordCount": recordCount,
            "durationMs": durationMs
        ]

        let body: [String: Any] = [
            "sdkVersion": OpenWearablesHealthSDK.sdkVersion,
            "provider": "apple",
            "events": [endEvent, collectDeviceStateEvent()]
        ]

        sendLogRequest(endpoint: endpoint, credential: credential, body: body) { }
    }

    // MARK: - Helper: fire end log when a type finishes during full export

    internal func fireTypeCompletedLog(_ typeIdentifier: String) {
        guard let startTime = fullSyncStartTime else { return }
        let recordCount = loadSyncState()?.typeProgress[typeIdentifier]?.sentCount ?? 0
        guard recordCount > 0 else { return }
        let durationMs = Int(Date().timeIntervalSince(startTime) * 1000)
        sendTypeEndLog(type: typeIdentifier, success: true, recordCount: recordCount, durationMs: durationMs)
    }

    // MARK: - Send Log Request

    private func sendLogRequest(
        endpoint: URL,
        credential: String,
        body: [String: Any],
        completion: @escaping () -> Void
    ) {
        guard let data = try? JSONSerialization.data(withJSONObject: body) else {
            logMessage("Failed to serialize sync log")
            completion()
            return
        }

        var req = URLRequest(url: endpoint)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        applyAuth(to: &req, credential: credential)
        req.httpBody = data

        let task = foregroundSession.dataTask(with: req) { [weak self] _, response, error in
            if let error = error {
                self?.logMessage("Sync log error: \(error.localizedDescription)")
            } else if let httpResponse = response as? HTTPURLResponse {
                self?.logMessage("Sync log: HTTP \(httpResponse.statusCode)")
            }
            completion()
        }
        task.resume()
    }
}
