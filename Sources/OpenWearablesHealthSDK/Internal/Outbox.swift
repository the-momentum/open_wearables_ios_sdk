import Foundation
import HealthKit

extension OpenWearablesHealthSDK {

    // MARK: - Outbox model
    internal struct OutboxItem: Codable {
        let typeIdentifier: String
        let userKey: String
        let payloadPath: String
        let anchorPath: String?
        let wasFullExport: Bool?
    }

    internal func outboxDir() -> URL {
        let base = try? FileManager.default.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
        return (base ?? FileManager.default.temporaryDirectory).appendingPathComponent("health_outbox", isDirectory: true)
    }

    internal func ensureOutboxDir() {
        try? FileManager.default.createDirectory(at: outboxDir(), withIntermediateDirectories: true)
    }

    internal func newPath(_ name: String, ext: String) -> URL {
        ensureOutboxDir()
        return outboxDir().appendingPathComponent("\(name).\(ext)")
    }

    // MARK: - Background upload with persistence
    internal func enqueueBackgroundUpload(
        payload: [String: Any],
        type: HKSampleType,
        candidateAnchor: HKQueryAnchor?,
        endpoint: URL,
        credential: String,
        completion: @escaping () -> Void
    ) {
        guard let data = try? JSONSerialization.data(withJSONObject: payload) else {
            completion()
            return
        }
        let id = UUID().uuidString
        let payloadURL = newPath("payload_\(id)", ext: "json")
        do {
            try data.write(to: payloadURL, options: Data.WritingOptions.atomic)
        } catch {
            completion()
            return
        }

        var anchorURL: URL? = nil
        if let cand = candidateAnchor,
           let ad = try? NSKeyedArchiver.archivedData(withRootObject: cand, requiringSecureCoding: true) {
            let u = newPath("anchor_\(id)", ext: "bin")
            try? ad.write(to: u, options: Data.WritingOptions.atomic)
            anchorURL = u
        }

        let item = OutboxItem(
            typeIdentifier: type.identifier,
            userKey: userKey(),
            payloadPath: payloadURL.path,
            anchorPath: anchorURL?.path,
            wasFullExport: nil
        )
        let itemURL = newPath("item_\(id)", ext: "json")
        if let md = try? JSONEncoder().encode(item) {
            try? md.write(to: itemURL, options: Data.WritingOptions.atomic)
        }

        var req = URLRequest(url: endpoint)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        applyAuth(to: &req, credential: credential)

        let task = session.uploadTask(with: req, fromFile: payloadURL)
        task.taskDescription = [itemURL.path, payloadURL.path, anchorURL?.path ?? ""].joined(separator: "|")
        task.resume()

        completion()
    }
    
    // MARK: - Combined upload
    internal func enqueueCombinedUpload(
        payload: [String: Any],
        anchors: [String: HKQueryAnchor],
        endpoint: URL,
        credential: String,
        wasFullExport: Bool = false,
        completion: @escaping (Bool) -> Void
    ) {
        guard let data = try? JSONSerialization.data(withJSONObject: payload) else {
            self.logMessage("Failed to serialize payload")
            completion(false)
            return
        }
        
        let id = UUID().uuidString
        let payloadURL = newPath("combined_payload_\(id)", ext: "json")
        
        do {
            try data.write(to: payloadURL, options: Data.WritingOptions.atomic)
        } catch {
            self.logMessage("Failed to write payload: \(error.localizedDescription)")
            completion(false)
            return
        }

        var anchorsURL: URL? = nil
        if !anchors.isEmpty {
            var anchorsData: [String: Data] = [:]
            for (typeId, anchor) in anchors {
                if let data = try? NSKeyedArchiver.archivedData(withRootObject: anchor, requiringSecureCoding: true) {
                    anchorsData[typeId] = data
                }
            }
            
            if let serializedData = try? NSKeyedArchiver.archivedData(withRootObject: anchorsData, requiringSecureCoding: true) {
                let u = newPath("combined_anchors_\(id)", ext: "bin")
                try? serializedData.write(to: u, options: Data.WritingOptions.atomic)
                anchorsURL = u
            }
        }

        let item = OutboxItem(
            typeIdentifier: "combined",
            userKey: userKey(),
            payloadPath: payloadURL.path,
            anchorPath: anchorsURL?.path,
            wasFullExport: wasFullExport
        )
        let itemURL = newPath("combined_item_\(id)", ext: "json")
        if let md = try? JSONEncoder().encode(item) {
            try? md.write(to: itemURL, options: Data.WritingOptions.atomic)
        }

        guard let payloadData = try? Data(contentsOf: payloadURL) else {
            self.logMessage("Failed to read payload")
            completion(false)
            return
        }
        
        var req = URLRequest(url: endpoint)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        applyAuth(to: &req, credential: credential)
        req.httpBody = payloadData
        req.setValue("\(payloadData.count)", forHTTPHeaderField: "Content-Length")
        
        self.logPayloadSummary(payloadData, label: "Sending")
        self.logPayloadToConsole(payloadData, label: "UPLOAD")

        let task = foregroundSession.dataTask(with: req) { [weak self] data, response, error in
            guard let self = self else { return }
            
            if let error = error {
                let nsError = error as NSError
                if nsError.code != NSURLErrorCancelled {
                    self.logMessage("Upload error: \(error.localizedDescription)")
                    self.markNetworkError()
                }
                try? FileManager.default.removeItem(atPath: payloadURL.path)
                completion(false)
                return
            }
            
            if let httpResponse = response as? HTTPURLResponse {
                if (200...299).contains(httpResponse.statusCode) {
                    self.logMessage("HTTP \(httpResponse.statusCode)")
                    
                    self.handleSuccessfulUpload(itemPath: itemURL.path, anchorPath: anchorsURL?.path, wasFullExport: wasFullExport)
                    
                    try? FileManager.default.removeItem(atPath: payloadURL.path)
                    completion(true)
                } else if httpResponse.statusCode == 401 {
                    self.handle401ForUpload(
                        payloadData: payloadData,
                        endpoint: endpoint,
                        itemPath: itemURL.path,
                        payloadPath: payloadURL.path,
                        anchorsPath: anchorsURL?.path,
                        wasFullExport: wasFullExport,
                        completion: completion
                    )
                } else {
                    var errorMsg = "HTTP \(httpResponse.statusCode)"
                    if let data = data, let errorBody = String(data: data, encoding: .utf8) {
                        let truncated = errorBody.count > 200 ? String(errorBody.prefix(200)) + "..." : errorBody
                        errorMsg += " - \(truncated)"
                    }
                    self.logMessage(errorMsg)
                    try? FileManager.default.removeItem(atPath: payloadURL.path)
                    
                    if (400...499).contains(httpResponse.statusCode) {
                        self.logMessage("Skipping chunk due to \(httpResponse.statusCode) - continuing sync")
                        completion(true)
                    } else {
                        completion(false)
                    }
                }
            } else {
                self.logMessage("No HTTP response")
                self.markNetworkError()
                try? FileManager.default.removeItem(atPath: payloadURL.path)
                completion(false)
            }
        }
        
        task.resume()
    }
    
    /// Handles 401 response for combined uploads.
    private func handle401ForUpload(
        payloadData: Data,
        endpoint: URL,
        itemPath: String,
        payloadPath: String,
        anchorsPath: String?,
        wasFullExport: Bool,
        completion: @escaping (Bool) -> Void
    ) {
        if isApiKeyAuth {
            self.logMessage("401 with API key - emitting auth error")
            self.emitAuthError(statusCode: 401)
            try? FileManager.default.removeItem(atPath: payloadPath)
            completion(false)
            return
        }
        
        self.attemptTokenRefresh { [weak self] refreshSuccess in
            guard let self = self else { return }
            
            if refreshSuccess, let newCredential = self.authCredential {
                self.logMessage("Retrying upload with refreshed token...")
                var retryReq = URLRequest(url: endpoint)
                retryReq.httpMethod = "POST"
                retryReq.setValue("application/json", forHTTPHeaderField: "Content-Type")
                self.applyAuth(to: &retryReq, credential: newCredential)
                retryReq.httpBody = payloadData
                retryReq.setValue("\(payloadData.count)", forHTTPHeaderField: "Content-Length")
                
                let retryTask = self.foregroundSession.dataTask(with: retryReq) { [weak self] retryData, retryResponse, retryError in
                    guard let self = self else { return }
                    
                    if let retryError = retryError {
                        self.logMessage("Retry upload error: \(retryError.localizedDescription)")
                        try? FileManager.default.removeItem(atPath: payloadPath)
                        completion(false)
                        return
                    }
                    
                    if let retryHttp = retryResponse as? HTTPURLResponse, (200...299).contains(retryHttp.statusCode) {
                        self.logMessage("Retry HTTP \(retryHttp.statusCode)")
                        self.handleSuccessfulUpload(itemPath: itemPath, anchorPath: anchorsPath, wasFullExport: wasFullExport)
                        try? FileManager.default.removeItem(atPath: payloadPath)
                        completion(true)
                    } else {
                        let retryStatus = (retryResponse as? HTTPURLResponse)?.statusCode ?? 0
                        self.logMessage("Retry failed: HTTP \(retryStatus)")
                        self.emitAuthError(statusCode: 401)
                        try? FileManager.default.removeItem(atPath: payloadPath)
                        completion(false)
                    }
                }
                retryTask.resume()
            } else {
                self.emitAuthError(statusCode: 401)
                try? FileManager.default.removeItem(atPath: payloadPath)
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
    internal func retryOutboxIfPossible() {
        guard let endpoint = self.syncEndpoint, let credential = self.authCredential else { return }
        let dir = outboxDir()
        guard let files = try? FileManager.default.contentsOfDirectory(at: dir, includingPropertiesForKeys: nil) else { return }
        
        let regularItems = files.filter { $0.lastPathComponent.hasPrefix("item_") && $0.pathExtension == "json" && !$0.lastPathComponent.hasPrefix("combined_item_") }
        let combinedItems = files.filter { $0.lastPathComponent.hasPrefix("combined_item_") && $0.pathExtension == "json" }

        for itemURL in regularItems {
            if let attrs = try? FileManager.default.attributesOfItem(atPath: itemURL.path),
               let mdate = attrs[.modificationDate] as? Date,
               Date().timeIntervalSince(mdate) < 30 {
                continue
            }
            guard let data = try? Data(contentsOf: itemURL),
                  let item = try? JSONDecoder().decode(OutboxItem.self, from: data) else { continue }
            let payloadURL = URL(fileURLWithPath: item.payloadPath)
            guard FileManager.default.fileExists(atPath: payloadURL.path),
                  let payloadData = try? Data(contentsOf: payloadURL) else { continue }
            
            var req = URLRequest(url: endpoint)
            req.httpMethod = "POST"
            req.setValue("application/json", forHTTPHeaderField: "Content-Type")
            applyAuth(to: &req, credential: credential)
            req.httpBody = payloadData
            req.setValue("\(payloadData.count)", forHTTPHeaderField: "Content-Length")

            let task = foregroundSession.dataTask(with: req) { data, response, error in
                if let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) {
                    try? FileManager.default.removeItem(atPath: payloadURL.path)
                    try? FileManager.default.removeItem(atPath: itemURL.path)
                }
            }
            task.resume()
        }
        
        for itemURL in combinedItems {
            if let attrs = try? FileManager.default.attributesOfItem(atPath: itemURL.path),
               let mdate = attrs[.modificationDate] as? Date,
               Date().timeIntervalSince(mdate) < 30 {
                continue
            }
            guard let itemData = try? Data(contentsOf: itemURL),
                  let item = try? JSONDecoder().decode(OutboxItem.self, from: itemData) else { continue }
            let payloadURL = URL(fileURLWithPath: item.payloadPath)
            guard FileManager.default.fileExists(atPath: payloadURL.path),
                  let payloadData = try? Data(contentsOf: payloadURL) else { continue }

            var req = URLRequest(url: endpoint)
            req.httpMethod = "POST"
            req.setValue("application/json", forHTTPHeaderField: "Content-Type")
            applyAuth(to: &req, credential: credential)
            req.httpBody = payloadData
            req.setValue("\(payloadData.count)", forHTTPHeaderField: "Content-Length")

            let task = foregroundSession.dataTask(with: req) { data, response, error in
                if let httpResponse = response as? HTTPURLResponse, (200...299).contains(httpResponse.statusCode) {
                    self.handleSuccessfulUpload(itemPath: itemURL.path, anchorPath: item.anchorPath, wasFullExport: item.wasFullExport ?? false)
                    try? FileManager.default.removeItem(atPath: payloadURL.path)
                }
            }
            task.resume()
        }
    }
}
