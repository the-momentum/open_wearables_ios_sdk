import Foundation
import UIKit
import HealthKit
import BackgroundTasks
import Network

/// Main entry point for the Open Wearables Health SDK.
/// Use `OpenWearablesHealthSDK.shared` to access the singleton instance.
///
/// This SDK handles:
/// - HealthKit authorization and data collection
/// - Background sync with streaming uploads
/// - Resumable sync sessions
/// - Dual authentication (token-based with auto-refresh, or API key)
/// - Persistent outbox for failed uploads
/// - Network and device lock monitoring
public final class OpenWearablesHealthSDK: NSObject, URLSessionDelegate, URLSessionTaskDelegate, URLSessionDataDelegate {

    /// Shared singleton instance.
    public static let shared = OpenWearablesHealthSDK()
    
    // MARK: - Public Callbacks
    
    /// Called whenever the SDK logs a message. Set this to receive log output.
    public var onLog: ((String) -> Void)?
    
    /// Called when an authentication error occurs (e.g., 401 Unauthorized).
    /// Parameters: (statusCode: Int, message: String)
    public var onAuthError: ((Int, String) -> Void)?

    // MARK: - Configuration State
    internal var host: String?
    
    // MARK: - User State (loaded from Keychain)
    internal var userId: String? { OpenWearablesHealthSdkKeychain.getUserId() }
    internal var accessToken: String? { OpenWearablesHealthSdkKeychain.getAccessToken() }
    internal var refreshToken: String? { OpenWearablesHealthSdkKeychain.getRefreshToken() }
    internal var apiKey: String? { OpenWearablesHealthSdkKeychain.getApiKey() }
    
    // Token refresh state
    private var isRefreshingToken = false
    private let tokenRefreshLock = NSLock()
    private var tokenRefreshCallbacks: [(Bool) -> Void] = []
    
    // MARK: - Auth Helpers
    
    internal var isApiKeyAuth: Bool {
        return apiKey != nil && accessToken == nil
    }
    
    internal var authCredential: String? {
        return accessToken ?? apiKey
    }
    
    internal var hasAuth: Bool {
        return authCredential != nil
    }
    
    internal func applyAuth(to request: inout URLRequest) {
        if let token = accessToken {
            request.setValue(token, forHTTPHeaderField: "Authorization")
        } else if let key = apiKey {
            request.setValue(key, forHTTPHeaderField: "X-Open-Wearables-API-Key")
        }
    }
    
    internal func applyAuth(to request: inout URLRequest, credential: String) {
        if isApiKeyAuth {
            request.setValue(credential, forHTTPHeaderField: "X-Open-Wearables-API-Key")
        } else {
            request.setValue(credential, forHTTPHeaderField: "Authorization")
        }
    }
    
    // MARK: - HealthKit State
    internal let healthStore = HKHealthStore()
    internal var session: URLSession!
    internal var foregroundSession: URLSession!
    internal var trackedTypes: [HKSampleType] = []
    internal var chunkSize: Int = 1000
    internal var backgroundChunkSize: Int = 100
    internal var recordsPerChunk: Int = 2000
    
    // Debouncing
    private var pendingSyncWorkItem: DispatchWorkItem?
    private let syncDebounceQueue = DispatchQueue(label: "health_sync_debounce")
    private var observerBgTask: UIBackgroundTaskIdentifier = .invalid
    
    // Sync flags
    internal var isInitialSyncInProgress = false
    private var isSyncing: Bool = false
    private var syncCancelled: Bool = false
    private let syncLock = NSLock()
    
    // Network monitoring
    private var networkMonitor: NWPathMonitor?
    private let networkMonitorQueue = DispatchQueue(label: "health_sync_network_monitor")
    private var wasDisconnected = false
    
    // Protected data monitoring
    private var protectedDataObserver: NSObjectProtocol?
    internal var pendingSyncAfterUnlock = false

    // Per-user state (anchors)
    internal let defaults = UserDefaults(suiteName: "com.openwearables.healthsdk.state") ?? .standard

    // Observer queries
    internal var activeObserverQueries: [HKObserverQuery] = []

    // Background session
    internal let bgSessionId = "com.openwearables.healthsdk.upload.session"

    // BGTask identifiers
    internal let refreshTaskId  = "com.openwearables.healthsdk.task.refresh"
    internal let processTaskId  = "com.openwearables.healthsdk.task.process"

    internal static var bgCompletionHandler: (() -> Void)?

    // Background response data buffer
    internal var backgroundDataBuffer: [Int: Data] = [:]
    private let bufferLock = NSLock()

    // MARK: - API Endpoints
    
    internal var apiBaseUrl: String? {
        guard let host = host else { return nil }
        let h = host.hasSuffix("/") ? String(host.dropLast()) : host
        return "\(h)/api/v1"
    }
    
    internal var syncEndpoint: URL? {
        guard let userId = userId else { return nil }
        guard let base = apiBaseUrl else { return nil }
        return URL(string: "\(base)/sdk/users/\(userId)/sync/apple")
    }
    
    // MARK: - Init
    
    private override init() {
        super.init()
        
        let bgCfg = URLSessionConfiguration.background(withIdentifier: bgSessionId)
        bgCfg.isDiscretionary = false
        bgCfg.waitsForConnectivity = true
        self.session = URLSession(configuration: bgCfg, delegate: self, delegateQueue: nil)
        
        let fgCfg = URLSessionConfiguration.default
        fgCfg.timeoutIntervalForRequest = 120
        fgCfg.timeoutIntervalForResource = 600
        fgCfg.waitsForConnectivity = false
        self.foregroundSession = URLSession(configuration: fgCfg, delegate: nil, delegateQueue: OperationQueue.main)

        if #available(iOS 13.0, *) {
            BGTaskScheduler.shared.register(forTaskWithIdentifier: refreshTaskId, using: nil) { [weak self] task in
                self?.handleAppRefresh(task: task as! BGAppRefreshTask)
            }
            BGTaskScheduler.shared.register(forTaskWithIdentifier: processTaskId, using: nil) { [weak self] task in
                self?.handleProcessing(task: task as! BGProcessingTask)
            }
        }
    }
    
    // MARK: - Public API: Background Completion Handler
    
    /// Set the background URL session completion handler (call from AppDelegate).
    public static func setBackgroundCompletionHandler(_ handler: @escaping () -> Void) {
        bgCompletionHandler = handler
    }
    
    // MARK: - Public API: Configure
    
    /// Initialize the SDK with the backend host URL.
    /// This also restores previously tracked types and auto-resumes sync if it was active.
    public func configure(host: String) {
        OpenWearablesHealthSdkKeychain.clearKeychainIfReinstalled()
        
        self.host = host
        OpenWearablesHealthSdkKeychain.saveHost(host)
        
        if let storedTypes = OpenWearablesHealthSdkKeychain.getTrackedTypes() {
            self.trackedTypes = mapTypesFromStrings(storedTypes)
            logMessage("Restored \(trackedTypes.count) tracked types")
        }
        
        logMessage("Configured: host=\(host)")
        
        if OpenWearablesHealthSdkKeychain.isSyncActive() && OpenWearablesHealthSdkKeychain.hasSession() && !trackedTypes.isEmpty {
            logMessage("Auto-restoring background sync...")
            DispatchQueue.main.async { [weak self] in
                self?.autoRestoreSync()
            }
        }
    }
    
    // MARK: - Public API: Authentication
    
    /// Sign in with user credentials. Provide either (accessToken + refreshToken) or apiKey.
    public func signIn(userId: String, accessToken: String?, refreshToken: String?, apiKey: String?) {
        let hasTokens = accessToken != nil && refreshToken != nil
        let hasApiKey = apiKey != nil
        
        guard hasTokens || hasApiKey else {
            logMessage("signIn error: Provide (accessToken + refreshToken) or (apiKey)")
            return
        }
        
        clearSyncSession()
        resetAllAnchors()
        clearOutbox()
        
        OpenWearablesHealthSdkKeychain.saveCredentials(userId: userId, accessToken: accessToken, refreshToken: refreshToken)
        
        if let apiKey = apiKey {
            OpenWearablesHealthSdkKeychain.saveApiKey(apiKey)
            logMessage("API key saved")
        }
        
        let authMode = hasTokens ? "token" : "apiKey"
        logMessage("Signed in: userId=\(userId), mode=\(authMode)")
    }
    
    /// Sign out - cancels sync, clears all state.
    public func signOut() {
        logMessage("Signing out")
        
        cancelSync()
        stopBackgroundDelivery()
        stopNetworkMonitoring()
        stopProtectedDataMonitoring()
        cancelAllBGTasks()
        resetAllAnchors()
        clearSyncSession()
        clearOutbox()
        OpenWearablesHealthSdkKeychain.clearAll()
        
        logMessage("Sign out complete - all sync state reset")
    }
    
    /// Update tokens (e.g., after external token refresh).
    public func updateTokens(accessToken: String, refreshToken: String?) {
        OpenWearablesHealthSdkKeychain.updateTokens(accessToken: accessToken, refreshToken: refreshToken)
        logMessage("Tokens updated")
        retryOutboxIfPossible()
    }
    
    /// Restore a previously saved session. Returns userId if restored, nil otherwise.
    public func restoreSession() -> String? {
        if OpenWearablesHealthSdkKeychain.hasSession(),
           let userId = OpenWearablesHealthSdkKeychain.getUserId() {
            logMessage("Session restored: userId=\(userId)")
            return userId
        }
        return nil
    }
    
    /// Whether a valid session exists in the Keychain.
    public var isSessionValid: Bool {
        return OpenWearablesHealthSdkKeychain.hasSession()
    }
    
    // MARK: - Public API: HealthKit Authorization
    
    /// Request HealthKit read authorization for the given health data types.
    ///
    /// ```swift
    /// sdk.requestAuthorization(types: [.steps, .heartRate, .sleep]) { granted in
    ///     print("Authorization granted: \(granted)")
    /// }
    /// ```
    public func requestAuthorization(types: [HealthDataType], completion: @escaping (Bool) -> Void) {
        self.trackedTypes = mapTypes(types)
        OpenWearablesHealthSdkKeychain.saveTrackedTypes(types.map { $0.rawValue })
        
        logMessage("Requesting auth for \(trackedTypes.count) types")
        
        requestAuthorizationInternal { ok in
            completion(ok)
        }
    }
    
    /// Request HealthKit read authorization using raw string identifiers.
    @available(*, deprecated, message: "Use requestAuthorization(types: [HealthDataType], completion:) instead")
    public func requestAuthorization(types: [String], completion: @escaping (Bool) -> Void) {
        let healthTypes = types.compactMap { HealthDataType(rawValue: $0) }
        requestAuthorization(types: healthTypes, completion: completion)
    }
    
    // MARK: - Public API: Sync
    
    /// Start background sync (registers HealthKit observers, schedules BG tasks, triggers initial sync).
    public func startBackgroundSync(completion: @escaping (Bool) -> Void) {
        guard userId != nil, hasAuth else {
            logMessage("Cannot start sync: not signed in")
            completion(false)
            return
        }
        
        startBackgroundDelivery()
        startNetworkMonitoring()
        startProtectedDataMonitoring()
        
        initialSyncKickoff { started in
            if started {
                self.logMessage("Sync started")
            } else {
                self.logMessage("Sync failed to start")
                self.isInitialSyncInProgress = false
            }
        }
        
        scheduleAppRefresh()
        scheduleProcessing()
        
        let canStart = HKHealthStore.isHealthDataAvailable() &&
                      self.syncEndpoint != nil &&
                      self.hasAuth &&
                      !self.trackedTypes.isEmpty
        
        if canStart {
            OpenWearablesHealthSdkKeychain.setSyncActive(true)
        }
        
        completion(canStart)
    }
    
    /// Stop background sync.
    public func stopBackgroundSync() {
        cancelSync()
        stopBackgroundDelivery()
        stopNetworkMonitoring()
        stopProtectedDataMonitoring()
        cancelAllBGTasks()
        OpenWearablesHealthSdkKeychain.setSyncActive(false)
    }
    
    /// Trigger an immediate sync.
    public func syncNow(completion: @escaping () -> Void) {
        syncAll(fullExport: false, completion: completion)
    }
    
    /// Whether sync is currently active.
    public var isSyncActive: Bool {
        return OpenWearablesHealthSdkKeychain.isSyncActive()
    }
    
    /// Get the current sync status.
    public func getSyncStatus() -> [String: Any] {
        return getSyncStatusDict()
    }
    
    /// Resume an interrupted sync session.
    public func resumeSync(completion: @escaping (Bool) -> Void) {
        guard hasResumableSyncSession() else {
            completion(false)
            return
        }
        
        syncAll(fullExport: false) {
            completion(true)
        }
    }
    
    /// Reset all sync anchors - forces full re-export on next sync.
    public func resetAnchors() {
        resetAllAnchors()
        clearSyncSession()
        clearOutbox()
        logMessage("Anchors reset - will perform full sync on next sync")
        
        if OpenWearablesHealthSdkKeychain.isSyncActive() && self.hasAuth {
            logMessage("Triggering full export after reset...")
            self.syncAll(fullExport: true) {
                self.logMessage("Full export after reset completed")
            }
        }
    }
    
    /// Get stored credentials.
    public func getStoredCredentials() -> [String: Any?] {
        return [
            "userId": OpenWearablesHealthSdkKeychain.getUserId(),
            "accessToken": OpenWearablesHealthSdkKeychain.getAccessToken(),
            "refreshToken": OpenWearablesHealthSdkKeychain.getRefreshToken(),
            "apiKey": OpenWearablesHealthSdkKeychain.getApiKey(),
            "host": OpenWearablesHealthSdkKeychain.getHost(),
            "isSyncActive": OpenWearablesHealthSdkKeychain.isSyncActive()
        ]
    }
    
    // MARK: - Internal: Auto Restore
    
    private func autoRestoreSync() {
        guard userId != nil, hasAuth else {
            logMessage("Cannot auto-restore: no session")
            return
        }
        
        startBackgroundDelivery()
        startNetworkMonitoring()
        startProtectedDataMonitoring()
        scheduleAppRefresh()
        scheduleProcessing()
        
        if hasResumableSyncSession() {
            logMessage("Found interrupted sync, will resume...")
            syncAll(fullExport: false) {
                self.logMessage("Resumed sync completed")
            }
        }
        
        logMessage("Background sync auto-restored")
    }

    // MARK: - Internal: Authorization
    
    internal func requestAuthorizationInternal(completion: @escaping (Bool) -> Void) {
        guard HKHealthStore.isHealthDataAvailable() else {
            DispatchQueue.main.async { completion(false) }
            return
        }
        
        let readTypes = Set(getQueryableTypes())
        logMessage("Requesting read-only auth for \(readTypes.count) types")
        
        healthStore.requestAuthorization(toShare: nil, read: readTypes) { ok, _ in
            DispatchQueue.main.async { completion(ok) }
        }
    }
    
    internal func getAuthCredential() -> String? {
        return authCredential
    }
    
    internal func getQueryableTypes() -> [HKSampleType] {
        let disallowedIdentifiers: Set<String> = [
            HKCorrelationTypeIdentifier.bloodPressure.rawValue
        ]
        
        return trackedTypes.filter { type in
            !disallowedIdentifiers.contains(type.identifier)
        }
    }

    // MARK: - Internal: Sync
    
    internal func syncAll(fullExport: Bool, completion: @escaping () -> Void) {
        guard !trackedTypes.isEmpty else { completion(); return }
        
        guard self.hasAuth else {
            self.logMessage("No auth credential for sync")
            completion()
            return
        }
        self.collectAllData(fullExport: fullExport, completion: completion)
    }
    
    internal func triggerCombinedSync() {
        if isInitialSyncInProgress {
            logMessage("Skipping - initial sync in progress")
            return
        }
        
        if observerBgTask == .invalid {
            observerBgTask = UIApplication.shared.beginBackgroundTask(withName: "health_combined_sync") {
                self.logMessage("Background task expired")
                UIApplication.shared.endBackgroundTask(self.observerBgTask)
                self.observerBgTask = .invalid
            }
        }
        
        pendingSyncWorkItem?.cancel()
        
        let workItem = DispatchWorkItem { [weak self] in
            guard let self = self else { return }
            self.syncAll(fullExport: false) {
                if self.observerBgTask != .invalid {
                    UIApplication.shared.endBackgroundTask(self.observerBgTask)
                    self.observerBgTask = .invalid
                }
            }
        }
        
        pendingSyncWorkItem = workItem
        syncDebounceQueue.asyncAfter(deadline: .now() + 2.0, execute: workItem)
    }
    
    internal func collectAllData(fullExport: Bool, completion: @escaping () -> Void) {
        collectAllData(fullExport: fullExport, isBackground: false, completion: completion)
    }
    
    internal func collectAllData(fullExport: Bool, isBackground: Bool, completion: @escaping () -> Void) {
        syncLock.lock()
        if isSyncing {
            logMessage("Sync in progress, skipping")
            syncLock.unlock()
            completion()
            return
        }
        isSyncing = true
        syncLock.unlock()
        
        guard HKHealthStore.isHealthDataAvailable() else {
            logMessage("HealthKit not available")
            finishSync()
            completion()
            return
        }
        
        guard let credential = self.authCredential, let endpoint = self.syncEndpoint else {
            logMessage("No auth credential or endpoint")
            finishSync()
            completion()
            return
        }
        
        let queryableTypes = getQueryableTypes()
        guard !queryableTypes.isEmpty else {
            logMessage("No queryable types")
            finishSync()
            completion()
            return
        }
        
        let typeNames = queryableTypes.map { shortTypeName($0.identifier) }.joined(separator: ", ")
        logMessage("Types to sync (\(queryableTypes.count)): \(typeNames)")
        
        let existingState = loadSyncState()
        let isResuming = existingState != nil && existingState!.hasProgress
        
        if isResuming {
            logMessage("Resuming sync (\(existingState!.totalSentCount) already sent, \(existingState!.completedTypes.count) types done)")
        } else {
            logMessage("Starting streaming sync (fullExport: \(fullExport), \(queryableTypes.count) types)")
            _ = startNewSyncState(fullExport: fullExport, types: queryableTypes)
        }
        
        let startIndex = isResuming ? getResumeTypeIndex() : 0
        
        processTypesSequentially(
            types: queryableTypes,
            typeIndex: startIndex,
            fullExport: fullExport,
            endpoint: endpoint,
            credential: credential,
            isBackground: isBackground
        ) { [weak self] allTypesCompleted in
            guard let self = self else { return }
            if allTypesCompleted {
                self.finalizeSyncState()
            } else {
                self.logMessage("Sync incomplete - will resume remaining types later")
            }
            self.finishSync()
            completion()
        }
    }
    
    private func processTypesSequentially(
        types: [HKSampleType],
        typeIndex: Int,
        fullExport: Bool,
        endpoint: URL,
        credential: String,
        isBackground: Bool,
        completion: @escaping (Bool) -> Void
    ) {
        syncLock.lock()
        let cancelled = syncCancelled
        syncLock.unlock()
        if cancelled {
            logMessage("Sync cancelled - stopping type processing")
            completion(false)
            return
        }
        
        guard typeIndex < types.count else {
            completion(true)
            return
        }
        
        let type = types[typeIndex]
        
        if !shouldSyncType(type.identifier) {
            logMessage("Skipping \(shortTypeName(type.identifier)) - already synced")
            processTypesSequentially(
                types: types, typeIndex: typeIndex + 1, fullExport: fullExport,
                endpoint: endpoint, credential: credential, isBackground: isBackground,
                completion: completion
            )
            return
        }
        
        updateCurrentTypeIndex(typeIndex)
        
        processTypeStreaming(
            type: type, fullExport: fullExport, endpoint: endpoint, credential: credential,
            chunkLimit: isBackground ? backgroundChunkSize : recordsPerChunk
        ) { [weak self] success in
            guard let self = self else { completion(false); return }
            
            if success {
                self.processTypesSequentially(
                    types: types, typeIndex: typeIndex + 1, fullExport: fullExport,
                    endpoint: endpoint, credential: credential, isBackground: isBackground,
                    completion: completion
                )
            } else {
                self.logMessage("Sync paused at \(self.shortTypeName(type.identifier)), will resume later")
                completion(false)
            }
        }
    }
    
    private func processTypeStreaming(
        type: HKSampleType, fullExport: Bool, endpoint: URL, credential: String,
        chunkLimit: Int, completion: @escaping (Bool) -> Void
    ) {
        let anchor = fullExport ? nil : loadAnchor(for: type)
        logMessage("\(shortTypeName(type.identifier)): querying...")
        
        let query = HKAnchoredObjectQuery(type: type, predicate: nil, anchor: anchor, limit: chunkLimit) {
            [weak self] _, samplesOrNil, deletedObjects, newAnchor, error in
            autoreleasepool {
                guard let self = self else { completion(false); return }
                
                self.syncLock.lock()
                let cancelled = self.syncCancelled
                self.syncLock.unlock()
                if cancelled { completion(false); return }
                
                if let error = error {
                    if self.isProtectedDataError(error) {
                        self.logMessage("\(self.shortTypeName(type.identifier)): protected data inaccessible - pausing sync")
                        self.pendingSyncAfterUnlock = true
                        completion(false)
                        return
                    }
                    self.logMessage("\(self.shortTypeName(type.identifier)): \(error.localizedDescription) - skipping")
                    self.updateTypeProgress(typeIdentifier: type.identifier, sentInChunk: 0, isComplete: true, anchorData: nil)
                    completion(true)
                    return
                }
                
                let samples = samplesOrNil ?? []
                if samples.isEmpty {
                    self.logMessage("  \(self.shortTypeName(type.identifier)): complete")
                    self.updateTypeProgress(typeIdentifier: type.identifier, sentInChunk: 0, isComplete: true, anchorData: nil)
                    completion(true)
                    return
                }
                
                self.logMessage("  \(self.shortTypeName(type.identifier)): \(samples.count) samples")
                let payload = self.serializeCombinedStreaming(samples: samples)
                
                var anchorData: Data? = nil
                if let newAnchor = newAnchor {
                    anchorData = try? NSKeyedArchiver.archivedData(withRootObject: newAnchor, requiringSecureCoding: true)
                }
                
                let isLastChunk = samples.count < chunkLimit
                
                self.sendChunkStreaming(
                    payload: payload, typeIdentifier: type.identifier, sampleCount: samples.count,
                    anchorData: anchorData, isLastChunk: isLastChunk, endpoint: endpoint, credential: credential
                ) { [weak self] success in
                    guard let self = self else { completion(false); return }
                    if success {
                        if isLastChunk {
                            completion(true)
                        } else {
                            self.processTypeStreamingContinue(
                                type: type, anchor: newAnchor, endpoint: endpoint,
                                credential: credential, chunkLimit: chunkLimit, completion: completion
                            )
                        }
                    } else {
                        completion(false)
                    }
                }
            }
        }
        
        healthStore.execute(query)
    }
    
    private func processTypeStreamingContinue(
        type: HKSampleType, anchor: HKQueryAnchor?, endpoint: URL,
        credential: String, chunkLimit: Int, completion: @escaping (Bool) -> Void
    ) {
        let query = HKAnchoredObjectQuery(type: type, predicate: nil, anchor: anchor, limit: chunkLimit) {
            [weak self] _, samplesOrNil, deletedObjects, newAnchor, error in
            autoreleasepool {
                guard let self = self else { completion(false); return }
                
                self.syncLock.lock()
                let cancelled = self.syncCancelled
                self.syncLock.unlock()
                if cancelled { completion(false); return }
                
                if let error = error {
                    if self.isProtectedDataError(error) {
                        self.logMessage("\(self.shortTypeName(type.identifier)): protected data inaccessible - pausing sync")
                        self.pendingSyncAfterUnlock = true
                        completion(false)
                        return
                    }
                    self.logMessage("\(self.shortTypeName(type.identifier)): \(error.localizedDescription) - skipping")
                    self.updateTypeProgress(typeIdentifier: type.identifier, sentInChunk: 0, isComplete: true, anchorData: nil)
                    completion(true)
                    return
                }
                
                let samples = samplesOrNil ?? []
                if samples.isEmpty {
                    self.updateTypeProgress(typeIdentifier: type.identifier, sentInChunk: 0, isComplete: true, anchorData: nil)
                    completion(true)
                    return
                }
                
                self.logMessage("  \(self.shortTypeName(type.identifier)): +\(samples.count) samples")
                let payload = self.serializeCombinedStreaming(samples: samples)
                
                var anchorData: Data? = nil
                if let newAnchor = newAnchor {
                    anchorData = try? NSKeyedArchiver.archivedData(withRootObject: newAnchor, requiringSecureCoding: true)
                }
                
                let isLastChunk = samples.count < chunkLimit
                
                self.sendChunkStreaming(
                    payload: payload, typeIdentifier: type.identifier, sampleCount: samples.count,
                    anchorData: anchorData, isLastChunk: isLastChunk, endpoint: endpoint, credential: credential
                ) { [weak self] success in
                    guard let self = self else { completion(false); return }
                    if success {
                        if isLastChunk {
                            completion(true)
                        } else {
                            self.processTypeStreamingContinue(
                                type: type, anchor: newAnchor, endpoint: endpoint,
                                credential: credential, chunkLimit: chunkLimit, completion: completion
                            )
                        }
                    } else {
                        completion(false)
                    }
                }
            }
        }
        
        healthStore.execute(query)
    }
    
    private func sendChunkStreaming(
        payload: [String: Any], typeIdentifier: String, sampleCount: Int,
        anchorData: Data?, isLastChunk: Bool, endpoint: URL, credential: String,
        completion: @escaping (Bool) -> Void
    ) {
        enqueueCombinedUpload(
            payload: payload, anchors: [:], endpoint: endpoint,
            credential: credential, wasFullExport: false
        ) { [weak self] success in
            guard let self = self else { completion(false); return }
            if success {
                self.updateTypeProgress(
                    typeIdentifier: typeIdentifier, sentInChunk: sampleCount,
                    isComplete: isLastChunk, anchorData: isLastChunk ? anchorData : nil
                )
            }
            completion(success)
        }
    }
    
    private func finishSync() {
        syncLock.lock()
        isSyncing = false
        isInitialSyncInProgress = false
        syncLock.unlock()
    }
    
    internal func cancelSync() {
        logMessage("Cancelling sync...")
        
        syncLock.lock()
        syncCancelled = true
        syncLock.unlock()
        
        pendingSyncWorkItem?.cancel()
        pendingSyncWorkItem = nil
        
        foregroundSession.getAllTasks { tasks in
            for task in tasks { task.cancel() }
        }
        session.getAllTasks { tasks in
            for task in tasks { task.cancel() }
        }
        
        if observerBgTask != .invalid {
            UIApplication.shared.endBackgroundTask(observerBgTask)
            observerBgTask = .invalid
        }
        
        finishSync()
        
        syncLock.lock()
        syncCancelled = false
        syncLock.unlock()
        
        logMessage("Sync cancelled")
    }
    
    internal func syncType(_ type: HKSampleType, fullExport: Bool, completion: @escaping () -> Void) {
        let anchor = fullExport ? nil : loadAnchor(for: type)
        
        let query = HKAnchoredObjectQuery(type: type, predicate: nil, anchor: anchor, limit: chunkSize) {
            [weak self] _, samplesOrNil, deletedObjects, newAnchor, error in
            guard let self = self else { completion(); return }
            guard error == nil else { completion(); return }

            let samples = samplesOrNil ?? []
            guard !samples.isEmpty else { completion(); return }
            
            guard let credential = self.authCredential, let endpoint = self.syncEndpoint else {
                completion()
                return
            }

            let payload = self.serialize(samples: samples, type: type)
            self.enqueueBackgroundUpload(payload: payload, type: type, candidateAnchor: newAnchor, endpoint: endpoint, credential: credential) {
                if samples.count == self.chunkSize {
                    self.syncType(type, fullExport: false, completion: completion)
                } else {
                    completion()
                }
            }
        }
        healthStore.execute(query)
    }
    
    // MARK: - Logging
    
    internal func logMessage(_ message: String) {
        NSLog("[OpenWearablesHealthSDK] %@", message)
        onLog?(message)
    }
    
    // MARK: - Token Refresh
    
    internal func attemptTokenRefresh(completion: @escaping (Bool) -> Void) {
        tokenRefreshLock.lock()
        
        if isRefreshingToken {
            tokenRefreshCallbacks.append(completion)
            tokenRefreshLock.unlock()
            return
        }
        
        guard let refreshToken = self.refreshToken, let base = self.apiBaseUrl else {
            tokenRefreshLock.unlock()
            logMessage("No refresh token or host - cannot refresh")
            completion(false)
            return
        }
        
        isRefreshingToken = true
        tokenRefreshCallbacks.append(completion)
        tokenRefreshLock.unlock()
        
        guard let url = URL(string: "\(base)/token/refresh") else {
            logMessage("Invalid refresh URL")
            finishTokenRefresh(success: false)
            return
        }
        
        logMessage("Attempting token refresh...")
        
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: String] = ["refresh_token": refreshToken]
        guard let bodyData = try? JSONSerialization.data(withJSONObject: body) else {
            logMessage("Failed to serialize refresh request body")
            finishTokenRefresh(success: false)
            return
        }
        req.httpBody = bodyData
        
        let task = foregroundSession.dataTask(with: req) { [weak self] data, response, error in
            guard let self = self else { return }
            
            if let error = error {
                self.logMessage("Token refresh failed: \(error.localizedDescription)")
                self.finishTokenRefresh(success: false)
                return
            }
            
            guard let httpResponse = response as? HTTPURLResponse,
                  (200...299).contains(httpResponse.statusCode),
                  let data = data else {
                let statusCode = (response as? HTTPURLResponse)?.statusCode ?? 0
                self.logMessage("Token refresh failed: HTTP \(statusCode)")
                self.finishTokenRefresh(success: false)
                return
            }
            
            guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let newAccessToken = json["access_token"] as? String else {
                self.logMessage("Token refresh: invalid response body")
                self.finishTokenRefresh(success: false)
                return
            }
            
            let newRefreshToken = json["refresh_token"] as? String
            OpenWearablesHealthSdkKeychain.updateTokens(accessToken: newAccessToken, refreshToken: newRefreshToken)
            
            self.logMessage("Token refreshed successfully")
            self.finishTokenRefresh(success: true)
        }
        
        task.resume()
    }
    
    private func finishTokenRefresh(success: Bool) {
        tokenRefreshLock.lock()
        let callbacks = tokenRefreshCallbacks
        tokenRefreshCallbacks = []
        isRefreshingToken = false
        tokenRefreshLock.unlock()
        
        for callback in callbacks {
            callback(success)
        }
    }
    
    // MARK: - Auth Error Emission
    
    internal func emitAuthError(statusCode: Int) {
        logMessage("Auth error: HTTP \(statusCode) - token invalid")
        onAuthError?(statusCode, "Unauthorized - please re-authenticate")
    }
    
    // MARK: - Payload Logging
    
    internal func logPayloadToConsole(_ data: Data, label: String) {
        #if DEBUG
        if let jsonObject = try? JSONSerialization.jsonObject(with: data, options: []),
           let prettyData = try? JSONSerialization.data(withJSONObject: jsonObject, options: [.prettyPrinted, .sortedKeys]),
           let prettyString = String(data: prettyData, encoding: .utf8) {
            NSLog("[OpenWearablesHealthSDK] ========== %@ PAYLOAD START ==========", label)
            let chunkSize = 800
            var index = prettyString.startIndex
            while index < prettyString.endIndex {
                let endIndex = prettyString.index(index, offsetBy: chunkSize, limitedBy: prettyString.endIndex) ?? prettyString.endIndex
                let chunk = String(prettyString[index..<endIndex])
                NSLog("[OpenWearablesHealthSDK] %@", chunk)
                index = endIndex
            }
            NSLog("[OpenWearablesHealthSDK] ========== %@ PAYLOAD END (%d bytes) ==========", label, data.count)
        }
        #endif
    }
    
    internal func logPayloadSummary(_ data: Data, label: String) {
        guard let jsonObject = try? JSONSerialization.jsonObject(with: data, options: []) as? [String: Any],
              let dataDict = jsonObject["data"] as? [String: Any] else {
            let sizeMB = Double(data.count) / (1024 * 1024)
            logMessage("\(label): \(String(format: "%.2f", sizeMB)) MB")
            return
        }
        
        var summary: [String] = []
        
        if let records = dataDict["records"] as? [[String: Any]] {
            var typeCounts: [String: Int] = [:]
            for record in records {
                if let type = record["type"] as? String {
                    let shortType = type.replacingOccurrences(of: "HKQuantityTypeIdentifier", with: "")
                        .replacingOccurrences(of: "HKCategoryTypeIdentifier", with: "")
                    typeCounts[shortType, default: 0] += 1
                }
            }
            if !typeCounts.isEmpty {
                let typesList = typeCounts.sorted { $0.value > $1.value }
                    .map { "\($0.key): \($0.value)" }.joined(separator: ", ")
                summary.append("Records: \(records.count) [\(typesList)]")
            }
        }
        
        if let workouts = dataDict["workouts"] as? [[String: Any]], !workouts.isEmpty {
            var workoutTypes: [String: Int] = [:]
            for workout in workouts {
                if let type = workout["type"] as? String { workoutTypes[type, default: 0] += 1 }
            }
            let workoutsList = workoutTypes.sorted { $0.value > $1.value }
                .map { "\($0.key): \($0.value)" }.joined(separator: ", ")
            summary.append("Workouts: \(workouts.count) [\(workoutsList)]")
        }
        
        let sizeMB = Double(data.count) / (1024 * 1024)
        let sizeStr = String(format: "%.2f MB", sizeMB)
        
        if summary.isEmpty {
            logMessage("\(label): \(sizeStr)")
        } else {
            logMessage("\(label): \(sizeStr) - \(summary.joined(separator: ", "))")
        }
    }
    
    // MARK: - Network Monitoring
    
    internal func startNetworkMonitoring() {
        guard networkMonitor == nil else { return }
        
        networkMonitor = NWPathMonitor()
        networkMonitor?.pathUpdateHandler = { [weak self] path in
            guard let self = self else { return }
            
            let isConnected = path.status == .satisfied
            
            if isConnected {
                if self.wasDisconnected {
                    self.wasDisconnected = false
                    self.logMessage("Network restored")
                    self.tryResumeAfterNetworkRestored()
                }
            } else {
                if !self.wasDisconnected {
                    self.wasDisconnected = true
                    self.logMessage("Network lost")
                }
            }
        }
        
        networkMonitor?.start(queue: networkMonitorQueue)
        logMessage("Network monitoring started")
    }
    
    internal func stopNetworkMonitoring() {
        networkMonitor?.cancel()
        networkMonitor = nil
        wasDisconnected = false
    }
    
    // MARK: - Protected Data Monitoring
    
    internal func startProtectedDataMonitoring() {
        guard protectedDataObserver == nil else { return }
        
        protectedDataObserver = NotificationCenter.default.addObserver(
            forName: UIApplication.protectedDataDidBecomeAvailableNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            guard let self = self else { return }
            self.logMessage("Device unlocked - protected data available")
            
            if self.pendingSyncAfterUnlock {
                self.pendingSyncAfterUnlock = false
                self.logMessage("Triggering deferred sync after unlock...")
                
                DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
                    guard let self = self else { return }
                    
                    self.syncLock.lock()
                    let alreadySyncing = self.isSyncing
                    self.syncLock.unlock()
                    
                    guard !alreadySyncing else {
                        self.logMessage("Sync already in progress after unlock")
                        return
                    }
                    
                    self.syncAll(fullExport: false) {
                        self.logMessage("Deferred sync after unlock completed")
                    }
                }
            }
        }
        
        logMessage("Protected data monitoring started")
    }
    
    internal func stopProtectedDataMonitoring() {
        if let observer = protectedDataObserver {
            NotificationCenter.default.removeObserver(observer)
            protectedDataObserver = nil
        }
        pendingSyncAfterUnlock = false
    }
    
    internal func markNetworkError() {
        wasDisconnected = true
    }
    
    private func tryResumeAfterNetworkRestored() {
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) { [weak self] in
            guard let self = self else { return }
            
            guard self.hasResumableSyncSession() else {
                self.logMessage("No sync to resume")
                return
            }
            
            self.syncLock.lock()
            let alreadySyncing = self.isSyncing
            self.syncLock.unlock()
            
            if alreadySyncing {
                self.logMessage("Sync already in progress")
                return
            }
            
            self.logMessage("Resuming sync after network restored...")
            self.syncAll(fullExport: false) {
                self.logMessage("Network resume sync completed")
            }
        }
    }
    
    // MARK: - Protected Data Error Detection
    
    internal func isProtectedDataError(_ error: Error) -> Bool {
        let nsError = error as NSError
        if nsError.domain == "com.apple.healthkit" && nsError.code == 6 {
            return true
        }
        let msg = error.localizedDescription.lowercased()
        return msg.contains("protected health data") || msg.contains("inaccessible")
    }
    
    // MARK: - Helpers
    
    internal func shortTypeName(_ identifier: String) -> String {
        return identifier
            .replacingOccurrences(of: "HKQuantityTypeIdentifier", with: "")
            .replacingOccurrences(of: "HKCategoryTypeIdentifier", with: "")
            .replacingOccurrences(of: "HKWorkoutType", with: "Workout")
    }
}

// MARK: - Array extension
extension Array {
    func chunked(into size: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}
