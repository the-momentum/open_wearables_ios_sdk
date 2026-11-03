import Foundation
import HealthKit

extension OpenWearablesHealthSDK {

    // MARK: - Outbox model

    /// Decoded, never encoded: the sync path stopped writing outbox items in 0.14, so
    /// the only items on disk are leftovers from an earlier SDK version.
    internal struct OutboxItem: Codable {
        let typeIdentifier: String
        let userKey: String
        let payloadPath: String
        let anchorPath: String?
        let wasFullExport: Bool?
    }

    /// Read-only. Nothing creates this directory anymore; when it is missing the
    /// enumerations in `clearOutbox` and `retryOutboxIfPossible` simply find nothing.
    internal func outboxDir() -> URL {
        return stateBaseDirectory().appendingPathComponent("health_outbox", isDirectory: true)
    }

    // MARK: - Combined upload
    
    /// Uploads one combined sync round.
    ///
    /// Nothing is written to the outbox. Progress lives in `SyncState` and advances
    /// only on a 2xx, so an interrupted round is rebuilt from HealthKit by the next
    /// sync. A persisted copy could only ever be replayed as a duplicate of data that
    /// the next sync re-fetches anyway, while `SyncState` knew nothing about it.
    /// Whether a combined-upload HTTP status should advance SyncState.
    /// Only 2xx means the server accepted the body. 4xx (including the production
    /// `ClientDisconnect` 400) must not move cursors — the chunk is rebuilt later.
    internal static func syncShouldAdvance(afterHTTPStatus statusCode: Int) -> Bool {
        (200...299).contains(statusCode)
    }
    
    internal func uploadCombinedPayload(
        payload: [String: Any],
        endpoint: URL,
        credential: String,
        generation: Int,
        syncContext: SyncRequestContext? = nil,
        completion: @escaping (Bool) -> Void
    ) {
        guard let payloadData = try? JSONSerialization.data(withJSONObject: payload) else {
            self.logMessage("Failed to serialize payload")
            completion(false)
            return
        }
        
        let requestId = UUID().uuidString
        var req = buildRequest(url: endpoint, credential: credential, requestId: requestId, syncContext: syncContext)
        req.httpBody = payloadData
        
        self.logPayloadSummary(payloadData, label: "Sending")
        
        let task = foregroundSession.dataTask(with: req) { [weak self] data, response, error in
            guard let self = self else { return }
            
            let completedTask = self.untrackSyncUpload(requestId: requestId)
            let statusCode = (response as? HTTPURLResponse)?.statusCode
            self.logUploadOutcome(
                stage: "sync", requestId: requestId, declaredBytes: payloadData.count,
                task: completedTask, statusCode: statusCode, error: error
            )
            
            if self.isSyncCancelled(generation: generation) {
                completion(false)
                return
            }
            
            if error != nil {
                self.markNetworkError()
                completion(false)
                return
            }
            
            guard let statusCode = statusCode else {
                self.logMessage("No HTTP response")
                self.markNetworkError()
                completion(false)
                return
            }
            
            if OpenWearablesHealthSDK.syncShouldAdvance(afterHTTPStatus: statusCode) {
                completion(true)
                return
            }
            
            if statusCode == 401 {
                self.handle401ForUpload(
                    payloadData: payloadData,
                    endpoint: endpoint,
                    requestId: requestId,
                    generation: generation,
                    syncContext: syncContext,
                    completion: completion
                )
                return
            }
            
            if let data = data, let errorBody = String(data: data, encoding: .utf8), !errorBody.isEmpty {
                let truncated = errorBody.count > 200 ? String(errorBody.prefix(200)) + "..." : errorBody
                self.logDiagnostic("HTTP \(statusCode) - \(truncated)")
            }
            
            completion(false)
        }
        
        trackSyncUpload(task, requestId: requestId)
        task.resume()
    }
    
    /// Handles 401 response for combined uploads. The retry reuses `requestId` so both
    /// attempts are one story in the server-side logs.
    private func handle401ForUpload(
        payloadData: Data,
        endpoint: URL,
        requestId: String,
        generation: Int,
        syncContext: SyncRequestContext?,
        completion: @escaping (Bool) -> Void
    ) {
        if isApiKeyAuth {
            self.logMessage("Got 401 with apiKey auth")
            self.emitAuthError(statusCode: 401)
            completion(false)
            return
        }
        
        self.logMessage("Got 401, refreshing token...")
        
        self.attemptTokenRefresh { [weak self] result in
            guard let self = self else { return }
            
            if self.isSyncCancelled(generation: generation) {
                completion(false)
                return
            }
            
            switch result {
            case .success:
                guard let newCredential = self.authCredential else {
                    self.logMessage("Token refreshed but no credential available")
                    completion(false)
                    return
                }
                self.logMessage("Token refreshed, retrying...")
                
                let retryKey = "\(requestId)#retry"
                var retryReq = self.buildRequest(
                    url: endpoint, credential: newCredential, requestId: requestId, syncContext: syncContext
                )
                retryReq.httpBody = payloadData
                
                let retryTask = self.foregroundSession.dataTask(with: retryReq) { [weak self] _, retryResponse, retryError in
                    guard let self = self else { return }
                    
                    let completedTask = self.untrackSyncUpload(requestId: retryKey)
                    let retryStatus = (retryResponse as? HTTPURLResponse)?.statusCode
                    self.logUploadOutcome(
                        stage: "sync-401-retry", requestId: requestId, declaredBytes: payloadData.count,
                        task: completedTask, statusCode: retryStatus, error: retryError
                    )
                    
                    if self.isSyncCancelled(generation: generation) {
                        completion(false)
                        return
                    }
                    
                    if retryError != nil {
                        self.markNetworkError()
                        completion(false)
                        return
                    }
                    
                    if let retryStatus = retryStatus, OpenWearablesHealthSDK.syncShouldAdvance(afterHTTPStatus: retryStatus) {
                        completion(true)
                        return
                    }
                    
                    if let retryStatus = retryStatus, (401...403).contains(retryStatus) {
                        self.emitAuthError(statusCode: retryStatus)
                    }
                    completion(false)
                }
                
                self.trackSyncUpload(retryTask, requestId: retryKey)
                retryTask.resume()
                
            case .authFailure:
                self.logMessage("Token refresh rejected - auth is invalid")
                self.emitAuthError(statusCode: 401)
                completion(false)
                
            case .networkError:
                self.logMessage("Token refresh failed (network) - will retry later")
                self.markNetworkError()
                completion(false)
            }
        }
    }
    
    // MARK: - Handle successful upload
    internal func handleSuccessfulUpload(itemPath: String, anchorPath: String?, wasFullExport: Bool) {
        guard let itemData = try? Data(contentsOf: URL(fileURLWithPath: itemPath)),
              let item = try? JSONDecoder().decode(OutboxItem.self, from: itemData) else {
            logMessage("Failed to read item for anchor saving")
            return
        }
        
        // signOut() clears credentials and the outbox asynchronously relative to
        // background-session callbacks. Do not persist anchors or fullDone for a
        // user who is gone, or for an item that belongs to a previous session.
        guard OpenWearablesHealthSdkKeychain.hasSession(), item.userKey == userKey() else {
            if let anchorPath = anchorPath, !anchorPath.isEmpty {
                try? FileManager.default.removeItem(atPath: anchorPath)
            }
            try? FileManager.default.removeItem(atPath: itemPath)
            return
        }
        
        if let anchorPath = anchorPath, !anchorPath.isEmpty {
            if item.typeIdentifier == "combined" {
                if let anchorData = try? Data(contentsOf: URL(fileURLWithPath: anchorPath)),
                   let anchorsDict = try? NSKeyedUnarchiver.unarchivedObject(ofClasses: [NSDictionary.self, NSString.self, NSData.self], from: anchorData) as? [String: Data] {
                    for (typeId, anchorData) in anchorsDict {
                        saveAnchorData(anchorData, typeIdentifier: typeId, userKey: item.userKey)
                    }
                    logMessage("Saved anchors for \(anchorsDict.count) types")
                }
            } else {
                if let anchorData = try? Data(contentsOf: URL(fileURLWithPath: anchorPath)) {
                    saveAnchorData(anchorData, typeIdentifier: item.typeIdentifier, userKey: item.userKey)
                }
            }
            
            try? FileManager.default.removeItem(atPath: anchorPath)
        }
        
        if wasFullExport {
            let fullDoneKey = "fullDone.\(item.userKey)"
            defaults.set(true, forKey: fullDoneKey)
            defaults.synchronize()
            logMessage("Marked full export complete")
        }
        
        try? FileManager.default.removeItem(atPath: itemPath)
    }

    // MARK: - Clear outbox
    internal func clearOutbox() {
        let dir = outboxDir()
        if let files = try? FileManager.default.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) {
            for file in files {
                try? FileManager.default.removeItem(at: file)
            }
        }
        logMessage("Cleared outbox")
    }

    // MARK: - Retry pending items
    
    /// Minimum file age before an outbox item is retried (the original upload may
    /// still be in flight).
    private static let outboxMinRetryAge: TimeInterval = 30
    /// Items older than this are dropped - the data is re-fetched from HealthKit by
    /// the regular sync anyway, so there is no point in re-sending week-old batches.
    private static let outboxMaxItemAge: TimeInterval = 7 * 24 * 3600
    
    /// Retries pending outbox items through the *background* URLSession.
    ///
    /// The sync path no longer enqueues items. This only drains leftovers from
    /// earlier SDK versions and then expires them.
    /// - uploads go through the background session (survive suspension/kill),
    /// - the session is limited to one connection per host, so items go out serially,
    /// - a retry pass is skipped while a regular sync is running,
    /// - only one retry pass can run at a time,
    /// - items already enqueued in the background session are not enqueued again.
    internal func retryOutboxIfPossible() {
        guard let endpoint = self.syncEndpoint, let credential = self.authCredential else { return }
        
        if isSyncInProgress {
            logMessage("Outbox retry skipped - sync in progress")
            return
        }
        
        outboxRetryLock.lock()
        if isRetryingOutbox {
            outboxRetryLock.unlock()
            return
        }
        isRetryingOutbox = true
        outboxRetryLock.unlock()
        
        session.getAllTasks { [weak self] tasks in
            guard let self = self else { return }
            defer {
                self.outboxRetryLock.lock()
                self.isRetryingOutbox = false
                self.outboxRetryLock.unlock()
            }
            
            // Payloads already queued in the background session (possibly from a
            // previous app run) must not be enqueued a second time.
            let inFlightPayloadPaths = Set(tasks.compactMap { task -> String? in
                let parts = task.taskDescription?.split(separator: "|", omittingEmptySubsequences: false)
                guard let parts = parts, parts.count > 1 else { return nil }
                return String(parts[1])
            })
            
            let dir = self.outboxDir()
            guard let files = try? FileManager.default.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) else { return }
            
            let itemFiles = files.filter {
                $0.pathExtension == "json" &&
                ($0.lastPathComponent.hasPrefix("item_") || $0.lastPathComponent.hasPrefix("combined_item_"))
            }
            
            var enqueued = 0
            for itemURL in itemFiles {
                guard let attrs = try? FileManager.default.attributesOfItem(atPath: itemURL.path),
                      let mdate = attrs[.modificationDate] as? Date else { continue }
                let age = Date().timeIntervalSince(mdate)
                if age < Self.outboxMinRetryAge { continue }
                
                guard let data = try? Data(contentsOf: itemURL),
                      let item = try? JSONDecoder().decode(OutboxItem.self, from: data) else {
                    try? FileManager.default.removeItem(at: itemURL)
                    continue
                }
                
                let payloadURL = URL(fileURLWithPath: item.payloadPath)
                guard FileManager.default.fileExists(atPath: payloadURL.path) else {
                    // Orphaned metadata without a payload - clean up
                    try? FileManager.default.removeItem(at: itemURL)
                    continue
                }
                
                if age > Self.outboxMaxItemAge {
                    self.logMessage("Outbox: dropping stale item (\(Int(age / 3600))h old)")
                    try? FileManager.default.removeItem(at: payloadURL)
                    if let anchorPath = item.anchorPath {
                        try? FileManager.default.removeItem(atPath: anchorPath)
                    }
                    try? FileManager.default.removeItem(at: itemURL)
                    continue
                }
                
                if inFlightPayloadPaths.contains(payloadURL.path) { continue }
                
                let itemId = itemURL.deletingPathExtension().lastPathComponent
                let req = self.buildRequest(
                    url: endpoint,
                    credential: credential,
                    requestId: UUID().uuidString,
                    outboxItemId: itemId
                )
                
                let task = self.session.uploadTask(with: req, fromFile: payloadURL)
                task.taskDescription = [itemURL.path, payloadURL.path, item.anchorPath ?? "", "\(self.currentSessionEpoch())"].joined(separator: "|")
                task.resume()
                enqueued += 1
            }
            
            if enqueued > 0 {
                self.logMessage("Outbox: enqueued \(enqueued) pending upload(s) to background session")
            }
        }
    }
}
