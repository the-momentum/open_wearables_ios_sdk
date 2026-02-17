import Foundation

extension OpenWearablesHealthSDK {

    // MARK: - URLSession delegate
    public func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        guard let desc = task.taskDescription else { return }
        let parts = desc.split(separator: "|", omittingEmptySubsequences: false).map(String.init)
        let itemPath = parts.count > 0 ? parts[0] : ""
        let payloadPath = parts.count > 1 ? parts[1] : ""
        let anchorPath = parts.count > 2 ? parts[2] : ""

        defer {
            if !payloadPath.isEmpty { try? FileManager.default.removeItem(atPath: payloadPath) }
            if error == nil, !itemPath.isEmpty { try? FileManager.default.removeItem(atPath: itemPath) }
        }

        if let error = error {
            let nsError = error as NSError
            if nsError.code != NSURLErrorCancelled {
                NSLog("[OpenWearablesHealthSDK] background upload failed: \(error.localizedDescription)")
            }
            return
        }

        if task.response is HTTPURLResponse {
            if backgroundDataBuffer[task.taskIdentifier] != nil {
                backgroundDataBuffer.removeValue(forKey: task.taskIdentifier)
            }
        }
        
        if let http = task.response as? HTTPURLResponse, !(200...299).contains(http.statusCode) {
            if http.statusCode == 401 {
                if isApiKeyAuth {
                    self.logMessage("Background 401 with API key - emitting auth error")
                    DispatchQueue.main.async { [weak self] in
                        self?.emitAuthError(statusCode: 401)
                    }
                } else {
                    self.attemptTokenRefresh { [weak self] refreshSuccess in
                        guard let self = self else { return }
                        if refreshSuccess {
                            self.logMessage("Token refreshed after background 401 - retrying outbox...")
                            self.retryOutboxIfPossible()
                        } else {
                            DispatchQueue.main.async { [weak self] in
                                self?.emitAuthError(statusCode: 401)
                            }
                        }
                    }
                }
            }
            return
        }

        if !itemPath.isEmpty,
           let itemData = try? Data(contentsOf: URL(fileURLWithPath: itemPath)),
           let item = try? JSONDecoder().decode(OutboxItem.self, from: itemData) {
            
            if item.typeIdentifier == "combined" {
                if !anchorPath.isEmpty,
                   let anchorData = try? Data(contentsOf: URL(fileURLWithPath: anchorPath)),
                   let anchorsDict = try? NSKeyedUnarchiver.unarchivedObject(ofClasses: [NSDictionary.self, NSString.self, NSData.self], from: anchorData) as? [String: Data] {
                    for (typeId, anchorData) in anchorsDict {
                        saveAnchorData(anchorData, typeIdentifier: typeId, userKey: item.userKey)
                    }
                }
                
                if item.wasFullExport == true {
                    let fullDoneKey = "fullDone.\(item.userKey)"
                    let defaults = UserDefaults(suiteName: "com.openwearables.healthsdk.state") ?? .standard
                    defaults.set(true, forKey: fullDoneKey)
                    defaults.synchronize()
                }
            } else {
                if !anchorPath.isEmpty,
                   let anchorData = try? Data(contentsOf: URL(fileURLWithPath: anchorPath)) {
                    saveAnchorData(anchorData, typeIdentifier: item.typeIdentifier, userKey: item.userKey)
                }
            }
            if !anchorPath.isEmpty {
                try? FileManager.default.removeItem(atPath: anchorPath)
            }
        }
    }

    public func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
        if let handler = OpenWearablesHealthSDK.bgCompletionHandler {
            OpenWearablesHealthSDK.bgCompletionHandler = nil
            handler()
        }
    }
    
    public func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
        let progress = Double(totalBytesSent) / Double(totalBytesExpectedToSend) * 100
        if Int(progress) % 20 == 0 || progress > 99 {
            NSLog("[OpenWearablesHealthSDK] Upload progress: \(String(format: "%.1f", progress))%% (\(totalBytesSent)/\(totalBytesExpectedToSend) bytes)")
        }
    }
    
    public func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive response: URLResponse, completionHandler: @escaping (URLSession.ResponseDisposition) -> Void) {
        completionHandler(.allow)
    }

    public func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        if backgroundDataBuffer[dataTask.taskIdentifier] == nil {
            backgroundDataBuffer[dataTask.taskIdentifier] = data
        } else {
            backgroundDataBuffer[dataTask.taskIdentifier]?.append(data)
        }
    }
}
