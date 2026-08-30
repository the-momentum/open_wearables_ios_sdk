import Foundation
import UIKit
import HealthKit
import BackgroundTasks
import Network

/// Controls which log messages the SDK emits.
///
/// - `none`:   No logs at all (neither console nor `onLog` callback).
/// - `always`: Logs are always emitted (console + callback).
/// - `debug`:  Logs are emitted only in debug builds (the default).
@objc public enum OWLogLevel: Int {
    case none = 0
    case always = 1
    case debug = 2
}

/// Distinguishes a genuine authentication failure from a transient network error
/// during token refresh so callers can decide whether to sign the user out.
internal enum TokenRefreshResult {
    /// Token was refreshed successfully.
    case success
    /// Refresh token is invalid — the server explicitly rejected it (HTTP 401/403).
    case authFailure
    /// Could not reach the server (timeout, DNS, connectivity, 5xx, etc.).
    case networkError
}

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
    
    internal static let sdkVersion = "0.14.0"
    
    // MARK: - Public Callbacks
    
    /// Called whenever the SDK logs a message. Set this to receive log output.
    public var onLog: ((String) -> Void)?
    
    /// Current log level. Default is `.debug` (logs only in debug builds).
    public var logLevel: OWLogLevel = .debug
    
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
    private var tokenRefreshCallbacks: [(TokenRefreshResult) -> Void] = []
    
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
    
    private func bearerValue(_ token: String) -> String {
        return token.hasPrefix("Bearer ") ? token : "Bearer \(token)"
    }
    
    internal func applyAuth(to request: inout URLRequest) {
        if let token = accessToken {
            request.setValue(bearerValue(token), forHTTPHeaderField: "Authorization")
        } else if let key = apiKey {
            request.setValue(key, forHTTPHeaderField: "X-Open-Wearables-API-Key")
        }
    }
    
    internal func applyAuth(to request: inout URLRequest, credential: String) {
        if isApiKeyAuth {
            request.setValue(credential, forHTTPHeaderField: "X-Open-Wearables-API-Key")
        } else {
            request.setValue(bearerValue(credential), forHTTPHeaderField: "Authorization")
        }
    }
    
    // MARK: - HealthKit State
    internal let healthStore = HKHealthStore()
    internal var session: URLSession!
    internal var foregroundSession: URLSession!
    internal var trackedTypes: [HKSampleType] = []
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
    internal var fullSyncStartTime: Date?
    
    internal var isSyncInProgress: Bool {
        syncLock.lock()
        defer { syncLock.unlock() }
        return isSyncing
    }
    
    // Outbox retry state
    internal var isRetryingOutbox = false
    internal let outboxRetryLock = NSLock()
    
    // Network monitoring
    private var networkMonitor: NWPathMonitor?
    private let networkMonitorQueue = DispatchQueue(label: "health_sync_network_monitor")
    private var wasDisconnected = false
    
    // Protected data monitoring
    private var protectedDataObserver: NSObjectProtocol?
    internal var pendingSyncAfterUnlock = false
    
    // Foreground monitoring (resume sync when app returns to foreground)
    private var foregroundObserver: NSObjectProtocol?

    // Per-user state (anchors)
    internal let defaults = UserDefaults(suiteName: "com.openwearables.healthsdk.state") ?? .standard

    // Observer queries
    internal var activeObserverQueries: [HKObserverQuery] = []
    internal let observerCompletionLock = NSLock()
    internal var observerCompletionHandlers: [() -> Void] = []

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
        return URL(string: "\(base)/sdk/users/\(userId)/sync")
    }
    
    // MARK: - Init
    
    private override init() {
        super.init()
        
        let bgCfg = URLSessionConfiguration.background(withIdentifier: bgSessionId)
        bgCfg.isDiscretionary = false
        bgCfg.waitsForConnectivity = true
        // Outbox retries go through this session. Serialize them (one connection at a
        // time instead of a parallel burst) and cap how long a stale batch may linger
        // in the system (default resource timeout is 7 days).
        bgCfg.httpMaximumConnectionsPerHost = 1
        bgCfg.timeoutIntervalForResource = 3600
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
        stopForegroundMonitoring()
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
    
    /// Computes the earliest date to sync from, based on persisted `syncDaysBack`.
    /// Returns the start of the day (midnight local time) that many days ago,
    /// or `nil` if full sync (no limit) is configured.
    internal func syncStartDate() -> Date? {
        let daysBack = OpenWearablesHealthSdkKeychain.getSyncDaysBack()
        guard daysBack > 0 else { return nil }
        let calendar = Calendar.current
        let startOfToday = calendar.startOfDay(for: Date())
        return calendar.date(byAdding: .day, value: -daysBack, to: startOfToday) ?? startOfToday
    }
    
    /// Start background sync (registers HealthKit observers, schedules BG tasks, triggers initial sync).
    ///
    /// - Parameters:
    ///   - syncDaysBack: How many days back to sync. Syncs from the start of the day
    ///     that many days ago (inclusive). When `nil` (the default), syncs all available history.
    ///   - completion: Called with `true` if sync started successfully.
    public func startBackgroundSync(syncDaysBack: Int? = nil, completion: @escaping (Bool) -> Void) {
        if let days = syncDaysBack {
            OpenWearablesHealthSdkKeychain.saveSyncDaysBack(days)
            logMessage("Sync days back set to \(days)")
        }
        guard userId != nil, hasAuth else {
            logMessage("Cannot start sync: not signed in")
            completion(false)
            return
        }
        
        startBackgroundDelivery()
        startNetworkMonitoring()
        startProtectedDataMonitoring()
        startForegroundMonitoring()
        
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
        stopForegroundMonitoring()
        cancelAllBGTasks()
        OpenWearablesHealthSdkKeychain.setSyncActive(false)
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
        startForegroundMonitoring()
        scheduleAppRefresh()
        scheduleProcessing()
        
        // Resume when there is a session with progress, but also when the initial
        // full export never completed (e.g. it was interrupted before its first
        // successful upload - such a session has no progress to detect).
        let fullDone = defaults.bool(forKey: fullDoneKey())
        if hasResumableSyncSession() || !fullDone {
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
    
    internal func triggerCombinedSync(observerCompletion: (() -> Void)? = nil) {
        if isInitialSyncInProgress {
            logMessage("Skipping observer-triggered sync - initial sync in progress")
            observerCompletion?()
            return
        }

        if let observerCompletion {
            observerCompletionLock.lock()
            observerCompletionHandlers.append(observerCompletion)
            observerCompletionLock.unlock()
        }
        
        if observerBgTask == .invalid {
            observerBgTask = UIApplication.shared.beginBackgroundTask(withName: "health_combined_sync") {
                self.logMessage("Background task expired - cancelling in-flight uploads")
                self.cancelInFlightForegroundUploads()
                self.finishObserverCompletions()
                UIApplication.shared.endBackgroundTask(self.observerBgTask)
                self.observerBgTask = .invalid
            }
        }
        
        pendingSyncWorkItem?.cancel()
        
        let workItem = DispatchWorkItem { [weak self] in
            guard let self = self else { return }
            self.syncAll(fullExport: false) {
                self.finishObserverCompletions()
                if self.observerBgTask != .invalid {
                    UIApplication.shared.endBackgroundTask(self.observerBgTask)
                    self.observerBgTask = .invalid
                }
            }
        }
        
        pendingSyncWorkItem = workItem
        syncDebounceQueue.asyncAfter(deadline: .now() + 2.0, execute: workItem)
    }
    
    internal func finishObserverCompletions() {
        observerCompletionLock.lock()
        let completions = observerCompletionHandlers
        observerCompletionHandlers.removeAll()
        observerCompletionLock.unlock()

        for completion in completions {
            completion()
        }
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
        
        guard self.authCredential != nil, let endpoint = self.syncEndpoint else {
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
        let fullDone = defaults.bool(forKey: fullDoneKey())

        let effectiveFullExport: Bool
        if let state = existingState, state.fullExport {
            effectiveFullExport = true
        } else if !fullDone {
            effectiveFullExport = true
        } else {
            effectiveFullExport = fullExport
        }
        if effectiveFullExport && !fullExport {
            logMessage("Escalating to full export (initial export not completed yet)")
        }
        
        if let state = existingState, state.fullExport == effectiveFullExport {
            if state.hasProgress {
                logMessage("Resuming sync (fullExport: \(effectiveFullExport), \(state.totalSentCount) already sent, \(state.completedTypes.count) types done)")
            } else {
                logMessage("Continuing sync session (fullExport: \(effectiveFullExport), no progress yet)")
            }
        } else {
            if let state = existingState {
                logMessage("Replacing session (fullExport: \(state.fullExport)) - starting streaming sync (fullExport: \(effectiveFullExport))")
            } else {
                logMessage("Starting streaming sync (fullExport: \(effectiveFullExport), \(queryableTypes.count) types)")
            }
            _ = startNewSyncState(fullExport: effectiveFullExport, types: queryableTypes)
        }
        
        let syncStartTime = Date()
        
        let startRoundRobin: () -> Void = { [weak self] in
            guard let self = self else { return }
            if effectiveFullExport {
                self.fullSyncStartTime = syncStartTime
            }
            self.processTypesRoundRobin(
                types: queryableTypes,
                fullExport: effectiveFullExport,
                endpoint: endpoint,
                isBackground: isBackground
            ) { [weak self] allTypesCompleted in
                guard let self = self else { return }
                
                if effectiveFullExport && !allTypesCompleted {
                    let durationMs = Int(Date().timeIntervalSince(syncStartTime) * 1000)
                    let state = self.loadSyncState()
                    for type in queryableTypes {
                        let typeId = type.identifier
                        if !(state?.completedTypes.contains(typeId) ?? false) {
                            let recordCount = state?.typeProgress[typeId]?.sentCount ?? 0
                            if recordCount > 0 {
                                self.sendTypeEndLog(type: typeId, success: false, recordCount: recordCount, durationMs: durationMs)
                            }
                        }
                    }
                }
                
                if allTypesCompleted {
                    self.finalizeSyncState()
                } else {
                    self.logMessage("Sync incomplete - will resume remaining types later")
                }
                self.fullSyncStartTime = nil
                self.finishSync()
                completion()
            }
        }
        
        if effectiveFullExport {
            let startDate = syncStartDate()
            let endDate = Date()
            // Start streaming immediately. A full HealthKit census across every tracked
            // type can take minutes or never complete on large, long-lived stores.
            sendSyncStartLog(types: queryableTypes, typeCounts: nil, startDate: startDate, endDate: endDate) { }
            // Telemetry must never gate HealthKit export. Start the functional path
            // immediately even if logging is slow, offline, or unavailable.
            startRoundRobin()
        } else {
            startRoundRobin()
        }
    }
    
    // MARK: - Round-Robin Sync Orchestration
    
    private class RoundRobinState {
        var olderThanCursors: [String: Date] = [:]
        var anchorCursors: [String: HKQueryAnchor] = [:]
        var completedTypes: Set<String> = []
    }
    
    private func processTypesRoundRobin(
        types: [HKSampleType],
        fullExport: Bool,
        endpoint: URL,
        isBackground: Bool,
        completion: @escaping (Bool) -> Void
    ) {
        let chunkLimit = isBackground ? backgroundChunkSize : recordsPerChunk
        let rrState = RoundRobinState()
        
        let resumeInfo = getResumeCursors()
        rrState.completedTypes = resumeInfo.completedTypes
        rrState.olderThanCursors = resumeInfo.olderThanCursors
        for (id, data) in resumeInfo.anchorDataCursors {
            if let anchor = try? NSKeyedUnarchiver.unarchivedObject(ofClass: HKQueryAnchor.self, from: data) {
                rrState.anchorCursors[id] = anchor
            }
        }
        
        if !fullExport {
            let fullDone = defaults.bool(forKey: fullDoneKey())
            for type in types where !rrState.completedTypes.contains(type.identifier) && rrState.anchorCursors[type.identifier] == nil {
                if let anchor = loadAnchor(for: type) {
                    rrState.anchorCursors[type.identifier] = anchor
                } else if !fullDone {
                    logMessage("\(shortTypeName(type.identifier)): no anchor and full export not completed - skipping incremental for this type")
                    rrState.completedTypes.insert(type.identifier)
                }
            }
        }
        
        processNextRound(
            types: types, fullExport: fullExport, endpoint: endpoint,
            chunkLimit: chunkLimit, rrState: rrState,
            completion: completion
        )
    }
    
    // MARK: - Round result for accumulating fetched data
    
    private struct TypeRoundResult {
        let type: HKSampleType
        let samples: [HKSample]
        let count: Int
        let nextOlderThan: Date?
        let newAnchor: HKQueryAnchor?
        let anchorData: Data?
        let isDone: Bool
    }
    
    // MARK: - Round-Robin with combined payloads
    
    private func processNextRound(
        types: [HKSampleType], fullExport: Bool, endpoint: URL,
        chunkLimit: Int, rrState: RoundRobinState,
        completion: @escaping (Bool) -> Void
    ) {
        syncLock.lock()
        let cancelled = syncCancelled
        syncLock.unlock()
        if cancelled {
            logMessage("Sync cancelled - stopping round-robin")
            completion(false)
            return
        }
        
        let incompleteTypes = types.filter { !rrState.completedTypes.contains($0.identifier) }
        if incompleteTypes.isEmpty {
            completion(true)
            return
        }
        
        let perTypeLimit = max(1, chunkLimit / incompleteTypes.count)
        
        // Phase 1: Fetch one chunk from each type (no network yet)
        fetchTypesInRound(
            types: incompleteTypes, index: 0, fullExport: fullExport,
            chunkLimit: perTypeLimit, rrState: rrState, accumulated: []
        ) { [weak self] success, results in
            guard let self = self else { completion(false); return }
            if !success { completion(false); return }
            
            // Update cursors for types that aren't done
            for result in results where !result.isDone {
                if fullExport {
                    rrState.olderThanCursors[result.type.identifier] = result.nextOlderThan
                } else if let anchor = result.newAnchor {
                    rrState.anchorCursors[result.type.identifier] = anchor
                }
            }
            
            // Mark empty/done types (no data to send) as complete immediately
            let emptyDone = results.filter { $0.samples.isEmpty && $0.isDone }
            for result in emptyDone {
                if !fullExport {
                    self.updateTypeProgress(typeIdentifier: result.type.identifier, sentInChunk: 0, isComplete: true, anchorData: result.anchorData)
                }
                rrState.completedTypes.insert(result.type.identifier)
                if fullExport { self.fireTypeCompletedLog(result.type.identifier) }
            }
            
            // Phase 2: Build combined payload from all types that returned data
            let withData = results.filter { !$0.samples.isEmpty }
            let allSamples = withData.flatMap { $0.samples }
            
            let doneTypesForAnchorCapture = results.filter { $0.isDone }.map { $0.type }
            
            if allSamples.isEmpty {
                if fullExport && !doneTypesForAnchorCapture.isEmpty {
                    self.captureAnchorsForDoneTypes(types: doneTypesForAnchorCapture, index: 0, rrState: rrState) { captureOk in
                        guard captureOk else { completion(false); return }
                        self.processNextRound(
                            types: types, fullExport: fullExport, endpoint: endpoint,
                            chunkLimit: chunkLimit, rrState: rrState,
                            completion: completion
                        )
                    }
                } else {
                    self.processNextRound(
                        types: types, fullExport: fullExport, endpoint: endpoint,
                        chunkLimit: chunkLimit, rrState: rrState,
                        completion: completion
                    )
                }
                return
            }
            
            guard let freshCredential = self.authCredential else {
                self.logMessage("No auth credential available for upload")
                completion(false)
                return
            }
            
            // When the app runs in the background it lives on a ~30s task assertion.
            // Don't start an upload that almost certainly cannot finish before
            // expiration. The margin is deliberately small (a chunk upload takes
            // ~1-3s) to use as much of the background window as possible: cursors
            // and anchors only advance on a server 2xx, so even if the very last
            // upload gets cut by expiration the batch is simply re-sent (and
            // deduplicated server-side) when the sync resumes.
            if let remaining = self.backgroundTimeRemainingIfInBackground(), remaining < 5 {
                self.logMessage("Background time low (\(Int(remaining))s left) - pausing sync before next upload")
                completion(false)
                return
            }
            
            let payload = self.serializeCombinedStreaming(samples: allSamples)
            
            self.enqueueCombinedUpload(
                payload: payload, anchors: [:], endpoint: endpoint,
                credential: freshCredential, wasFullExport: false
            ) { [weak self] sendSuccess in
                guard let self = self else { completion(false); return }
                if !sendSuccess { completion(false); return }
                
                // Phase 3: Update progress for all types that had data
                for result in withData {
                    if fullExport {
                        self.updateTypeProgress(
                            typeIdentifier: result.type.identifier, sentInChunk: result.count,
                            isComplete: false, anchorData: nil, olderThan: result.nextOlderThan
                        )
                    } else {
                        self.updateTypeProgress(
                            typeIdentifier: result.type.identifier, sentInChunk: result.count,
                            isComplete: result.isDone, anchorData: result.anchorData
                        )
                        if result.isDone {
                            rrState.completedTypes.insert(result.type.identifier)
                        }
                    }
                }
                
                // Phase 4: For full export, capture anchors for done types
                let fullExportDone = withData.filter { $0.isDone }.map { $0.type } + doneTypesForAnchorCapture.filter { t in !withData.contains(where: { $0.type == t }) }
                if fullExport && !fullExportDone.isEmpty {
                    self.captureAnchorsForDoneTypes(types: fullExportDone, index: 0, rrState: rrState) { captureOk in
                        guard captureOk else { completion(false); return }
                        self.processNextRound(
                            types: types, fullExport: fullExport, endpoint: endpoint,
                            chunkLimit: chunkLimit, rrState: rrState,
                            completion: completion
                        )
                    }
                } else {
                    self.processNextRound(
                        types: types, fullExport: fullExport, endpoint: endpoint,
                        chunkLimit: chunkLimit, rrState: rrState,
                        completion: completion
                    )
                }
            }
        }
    }
    
    // MARK: - Fetch all types in a round (no network, accumulates results)
    
    private func fetchTypesInRound(
        types: [HKSampleType], index: Int, fullExport: Bool,
        chunkLimit: Int, rrState: RoundRobinState,
        accumulated: [TypeRoundResult],
        completion: @escaping (Bool, [TypeRoundResult]) -> Void
    ) {
        guard index < types.count else {
            completion(true, accumulated)
            return
        }
        
        let type = types[index]
        
        if fullExport {
            let cursor = rrState.olderThanCursors[type.identifier]
            fetchOneChunkNewestFirst(type: type, olderThan: cursor, chunkLimit: chunkLimit) {
                [weak self] success, samples, nextOlderThan, isDone in
                guard let self = self else { completion(false, accumulated); return }
                if !success { completion(false, accumulated); return }
                
                let result = TypeRoundResult(
                    type: type, samples: samples, count: samples.count,
                    nextOlderThan: nextOlderThan, newAnchor: nil, anchorData: nil, isDone: isDone
                )
                self.fetchTypesInRound(
                    types: types, index: index + 1, fullExport: fullExport,
                    chunkLimit: chunkLimit, rrState: rrState,
                    accumulated: accumulated + [result], completion: completion
                )
            }
        } else {
            let anchor = rrState.anchorCursors[type.identifier]
            fetchOneChunkIncremental(type: type, anchor: anchor, chunkLimit: chunkLimit) {
                [weak self] success, samples, newAnchor, anchorData, isDone in
                guard let self = self else { completion(false, accumulated); return }
                if !success { completion(false, accumulated); return }
                
                let result = TypeRoundResult(
                    type: type, samples: samples, count: samples.count,
                    nextOlderThan: nil, newAnchor: newAnchor, anchorData: anchorData, isDone: isDone
                )
                self.fetchTypesInRound(
                    types: types, index: index + 1, fullExport: fullExport,
                    chunkLimit: chunkLimit, rrState: rrState,
                    accumulated: accumulated + [result], completion: completion
                )
            }
        }
    }
    
    // MARK: - Capture anchors for completed full-export types
    
    private func captureAnchorsForDoneTypes(
        types: [HKSampleType], index: Int, rrState: RoundRobinState,
        completion: @escaping (Bool) -> Void
    ) {
        guard index < types.count else {
            completion(true)
            return
        }
        
        let type = types[index]
        captureCurrentAnchor(for: type) { [weak self] anchor in
            guard let self = self else { completion(false); return }
            
            // Without a valid anchor the type must NOT be marked complete: the next
            // incremental sync would run an anchored query from nil and re-crawl the
            // whole history oldest-first. Pause the sync instead - on resume the type
            // re-sends its last chunk (deduplicated server-side) and retries capture.
            guard let anchor = anchor,
                  let anchorData = try? NSKeyedArchiver.archivedData(withRootObject: anchor, requiringSecureCoding: true) else {
                self.logMessage("  \(self.shortTypeName(type.identifier)): anchor capture failed - leaving type incomplete, pausing sync")
                completion(false)
                return
            }
            
            self.updateTypeProgress(typeIdentifier: type.identifier, sentInChunk: 0, isComplete: true, anchorData: anchorData)
            rrState.completedTypes.insert(type.identifier)
            self.fireTypeCompletedLog(type.identifier)
            self.logMessage("  \(self.shortTypeName(type.identifier)): complete (anchor captured)")
            self.captureAnchorsForDoneTypes(types: types, index: index + 1, rrState: rrState, completion: completion)
        }
    }
    
    // MARK: - Fetch-Only Chunk Processors (no network)
    
    private func fetchOneChunkNewestFirst(
        type: HKSampleType, olderThan: Date?, chunkLimit: Int,
        completion: @escaping (_ success: Bool, _ samples: [HKSample], _ nextOlderThan: Date?, _ isDone: Bool) -> Void
    ) {
        syncLock.lock()
        let cancelled = syncCancelled
        syncLock.unlock()
        if cancelled { completion(false, [], nil, false); return }
        
        let startDate = syncStartDate()
        var predicate: NSPredicate? = nil
        if let olderThan = olderThan {
            predicate = HKQuery.predicateForSamples(withStart: startDate, end: olderThan, options: .strictEndDate)
        } else if startDate != nil {
            predicate = HKQuery.predicateForSamples(withStart: startDate, end: nil, options: [])
        }
        
        let sortDescriptor = NSSortDescriptor(key: HKSampleSortIdentifierEndDate, ascending: false)
        
        let query = HKSampleQuery(sampleType: type, predicate: predicate, limit: chunkLimit, sortDescriptors: [sortDescriptor]) {
            [weak self] _, samplesOrNil, error in
            autoreleasepool {
                guard let self = self else { completion(false, [], nil, false); return }
                
                self.syncLock.lock()
                let cancelled = self.syncCancelled
                self.syncLock.unlock()
                if cancelled { completion(false, [], nil, false); return }
                
                if let error = error {
                    if self.isProtectedDataError(error) {
                        self.logMessage("\(self.shortTypeName(type.identifier)): protected data inaccessible - pausing sync")
                        self.pendingSyncAfterUnlock = true
                        completion(false, [], nil, false)
                        return
                    }
                    self.logMessage("\(self.shortTypeName(type.identifier)): \(error.localizedDescription) - skipping")
                    completion(true, [], nil, true)
                    return
                }
                
                let samples = samplesOrNil ?? []
                if samples.isEmpty {
                    self.logMessage("  \(self.shortTypeName(type.identifier)): all data sent (newest first)")
                    completion(true, [], nil, true)
                    return
                }
                
                let isLastChunk = samples.count < chunkLimit
                let nextOlderThan = isLastChunk ? nil : samples.last!.endDate
                self.logMessage("  \(self.shortTypeName(type.identifier)): \(samples.count) samples (newest first)")
                completion(true, samples, nextOlderThan, isLastChunk)
            }
        }
        
        healthStore.execute(query)
    }
    
    private func fetchOneChunkIncremental(
        type: HKSampleType, anchor: HKQueryAnchor?, chunkLimit: Int,
        completion: @escaping (_ success: Bool, _ samples: [HKSample], _ newAnchor: HKQueryAnchor?, _ anchorData: Data?, _ isDone: Bool) -> Void
    ) {
        let syncPredicate: NSPredicate? = {
            guard let start = syncStartDate() else { return nil }
            return HKQuery.predicateForSamples(withStart: start, end: nil, options: [])
        }()
        
        let query = HKAnchoredObjectQuery(type: type, predicate: syncPredicate, anchor: anchor, limit: chunkLimit) {
            [weak self] _, samplesOrNil, deletedObjects, newAnchor, error in
            autoreleasepool {
                guard let self = self else { completion(false, [], nil, nil, false); return }
                
                self.syncLock.lock()
                let cancelled = self.syncCancelled
                self.syncLock.unlock()
                if cancelled { completion(false, [], nil, nil, false); return }
                
                if let error = error {
                    if self.isProtectedDataError(error) {
                        self.logMessage("\(self.shortTypeName(type.identifier)): protected data inaccessible - pausing sync")
                        self.pendingSyncAfterUnlock = true
                        completion(false, [], nil, nil, false)
                        return
                    }
                    self.logMessage("\(self.shortTypeName(type.identifier)): \(error.localizedDescription) - skipping")
                    completion(true, [], nil, nil, true)
                    return
                }
                
                let samples = samplesOrNil ?? []
                let deletedCount = deletedObjects?.count ?? 0
                
                var anchorData: Data? = nil
                if let newAnchor = newAnchor {
                    anchorData = try? NSKeyedArchiver.archivedData(withRootObject: newAnchor, requiringSecureCoding: true)
                }
                
                if samples.isEmpty && deletedCount == 0 {
                    self.logMessage("  \(self.shortTypeName(type.identifier)): complete")
                    completion(true, [], newAnchor, anchorData, true)
                    return
                }
                
                // Deleted objects count against the query limit. Ignoring them made a
                // chunk with samples + deletions look like the last one, so the anchor
                // for the remaining (unfetched) data was never advanced.
                let isLastChunk = (samples.count + deletedCount) < chunkLimit
                self.logMessage("  \(self.shortTypeName(type.identifier)): \(samples.count) samples" + (deletedCount > 0 ? ", \(deletedCount) deleted" : ""))
                completion(true, samples, newAnchor, anchorData, isLastChunk)
            }
        }
        
        healthStore.execute(query)
    }
    
    // MARK: - Anchor Capture (for incremental sync after full export)
    
    private func captureCurrentAnchor(for type: HKSampleType, completion: @escaping (HKQueryAnchor?) -> Void) {
        logMessage("  \(shortTypeName(type.identifier)): saving anchor...")
        captureAnchorStep(type: type, anchor: nil, limit: 10000, completion: completion)
    }
    
    private func captureAnchorStep(type: HKSampleType, anchor: HKQueryAnchor?, limit: Int, completion: @escaping (HKQueryAnchor?) -> Void) {
        let syncPredicate: NSPredicate? = {
            guard let start = syncStartDate() else { return nil }
            return HKQuery.predicateForSamples(withStart: start, end: nil, options: [])
        }()
        let query = HKAnchoredObjectQuery(type: type, predicate: syncPredicate, anchor: anchor, limit: limit) {
            [weak self] _, samples, deletedObjects, newAnchor, error in
            guard let self = self else { completion(nil); return }
            
            if let error = error {
                // A failed step must not return a stale/nil anchor - the caller would
                // persist it and the next incremental sync would re-crawl history.
                if self.isProtectedDataError(error) {
                    self.logMessage("\(self.shortTypeName(type.identifier)): protected data inaccessible during anchor capture - will retry after unlock")
                    self.pendingSyncAfterUnlock = true
                } else {
                    self.logMessage("\(self.shortTypeName(type.identifier)): anchor capture failed - \(error.localizedDescription)")
                }
                completion(nil)
                return
            }
            
            // Deleted objects count against the query limit too. Ignoring them made
            // the recursion stop early with an anchor that wasn't fully advanced.
            let count = (samples?.count ?? 0) + (deletedObjects?.count ?? 0)
            if count >= limit {
                self.captureAnchorStep(type: type, anchor: newAnchor, limit: limit, completion: completion)
            } else {
                completion(newAnchor)
            }
        }
        healthStore.execute(query)
    }
    
    
    private func finishSync() {
        syncLock.lock()
        isSyncing = false
        isInitialSyncInProgress = false
        syncLock.unlock()
    }
    
    /// Returns the remaining background execution time when the app is in the
    /// background, or `nil` when it is active (foreground has no time limit).
    /// UIApplication state must be read on the main thread; sync uploads complete
    /// on the main queue, so guard against deadlocking with `Thread.isMainThread`.
    private func backgroundTimeRemainingIfInBackground() -> TimeInterval? {
        let read: () -> TimeInterval? = {
            guard UIApplication.shared.applicationState == .background else { return nil }
            return UIApplication.shared.backgroundTimeRemaining
        }
        if Thread.isMainThread {
            return read()
        }
        return DispatchQueue.main.sync(execute: read)
    }
    
    /// Cancels in-flight foreground uploads. Called from BG task expiration handlers
    /// so requests are terminated cleanly instead of being frozen mid-transfer when
    /// the app gets suspended (which leaves the server with a dangling connection).
    internal func cancelInFlightForegroundUploads() {
        foregroundSession.getAllTasks { tasks in
            for task in tasks { task.cancel() }
        }
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
    
    // MARK: - Logging
    
    /// Sets the log level. Convenience wrapper for Objective-C / Flutter bridge.
    public func setLogLevel(_ level: OWLogLevel) {
        self.logLevel = level
    }
    
    internal func logMessage(_ message: String) {
        switch logLevel {
        case .none:
            return
        case .always:
            break
        case .debug:
            #if !DEBUG
            return
            #endif
        }
        NSLog("[OpenWearablesHealthSDK] %@", message)
        onLog?(message)
    }
    
    // MARK: - Token Refresh
    
    internal func attemptTokenRefresh(completion: @escaping (TokenRefreshResult) -> Void) {
        tokenRefreshLock.lock()
        
        if isRefreshingToken {
            tokenRefreshCallbacks.append(completion)
            tokenRefreshLock.unlock()
            return
        }
        
        guard let refreshToken = self.refreshToken, let base = self.apiBaseUrl else {
            tokenRefreshLock.unlock()
            logMessage("Token refresh failed: no credentials")
            completion(.authFailure)
            return
        }
        
        isRefreshingToken = true
        tokenRefreshCallbacks.append(completion)
        tokenRefreshLock.unlock()
        
        guard let url = URL(string: "\(base)/token/refresh") else {
            logMessage("Token refresh failed: invalid URL")
            finishTokenRefresh(result: .authFailure)
            return
        }
        
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        
        let body: [String: String] = ["refresh_token": refreshToken]
        guard let bodyData = try? JSONSerialization.data(withJSONObject: body) else {
            logMessage("Token refresh failed: serialization error")
            finishTokenRefresh(result: .networkError)
            return
        }
        req.httpBody = bodyData
        
        let task = foregroundSession.dataTask(with: req) { [weak self] data, response, error in
            guard let self = self else { return }
            
            if let error = error {
                self.logMessage("Token refresh failed: \(error.localizedDescription)")
                self.finishTokenRefresh(result: .networkError)
                return
            }
            
            let statusCode = (response as? HTTPURLResponse)?.statusCode ?? 0
            
            if (401...403).contains(statusCode) {
                self.logMessage("Token refresh rejected: HTTP \(statusCode)")
                self.finishTokenRefresh(result: .authFailure)
                return
            }
            
            guard let httpResponse = response as? HTTPURLResponse,
                  (200...299).contains(httpResponse.statusCode),
                  let data = data else {
                self.logMessage("Token refresh failed: HTTP \(statusCode)")
                self.finishTokenRefresh(result: .networkError)
                return
            }
            
            guard let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let newAccessToken = json["access_token"] as? String else {
                self.logMessage("Token refresh failed: invalid response body")
                self.finishTokenRefresh(result: .networkError)
                return
            }
            
            let newRefreshToken = json["refresh_token"] as? String
            OpenWearablesHealthSdkKeychain.updateTokens(accessToken: newAccessToken, refreshToken: newRefreshToken)
            self.logMessage("Token refresh: HTTP \(statusCode)")
            self.finishTokenRefresh(result: .success)
        }
        
        task.resume()
    }
    
    private func finishTokenRefresh(result: TokenRefreshResult) {
        tokenRefreshLock.lock()
        let callbacks = tokenRefreshCallbacks
        tokenRefreshCallbacks = []
        isRefreshingToken = false
        tokenRefreshLock.unlock()
        
        for callback in callbacks {
            callback(result)
        }
    }
    
    // MARK: - Auth Error Emission
    
    internal func emitAuthError(statusCode: Int) {
        logMessage("Auth error: HTTP \(statusCode) - token invalid")
        onAuthError?(statusCode, "Unauthorized - please re-authenticate")
    }
    
    // MARK: - Payload Logging
    
    internal func logPayloadSummary(_ data: Data, label: String) {
        let sizeKB = Double(data.count) / 1024
        
        guard let jsonObject = try? JSONSerialization.jsonObject(with: data, options: []) as? [String: Any],
              let dataDict = jsonObject["data"] as? [String: Any] else {
            logMessage("\(label): \(String(format: "%.0f", sizeKB)) KB")
            return
        }
        
        var typeCounts: [String: Int] = [:]
        
        if let records = dataDict["records"] as? [[String: Any]] {
            for record in records {
                guard let type = record["type"] as? String else { continue }
                let shortType = type
                    .replacingOccurrences(of: "HKQuantityTypeIdentifier", with: "")
                    .replacingOccurrences(of: "HKCategoryTypeIdentifier", with: "")
                typeCounts[shortType, default: 0] += 1
            }
        }
        if let sleep = dataDict["sleep"] as? [[String: Any]], !sleep.isEmpty {
            typeCounts["sleep"] = sleep.count
        }
        if let workouts = dataDict["workouts"] as? [[String: Any]], !workouts.isEmpty {
            typeCounts["workouts"] = workouts.count
        }
        
        let totalCount = typeCounts.values.reduce(0, +)
        let breakdown = typeCounts
            .sorted { $0.value > $1.value }
            .map { "\($0.key): \($0.value)" }
            .joined(separator: ", ")
        
        logMessage("\(label) \(String(format: "%.0f", sizeKB)) KB, \(totalCount) items (\(breakdown))")
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
    
    // MARK: - Foreground Monitoring
    
    /// Resumes an interrupted sync when the app returns to the foreground.
    /// A sync paused in the background (e.g. "Background time low") had no
    /// trigger to continue once the user reopened the app - observers only fire
    /// on new HealthKit data and BG tasks run opportunistically much later.
    internal func startForegroundMonitoring() {
        guard foregroundObserver == nil else { return }
        
        foregroundObserver = NotificationCenter.default.addObserver(
            forName: UIApplication.didBecomeActiveNotification,
            object: nil,
            queue: .main
        ) { [weak self] _ in
            self?.tryResumeAfterForeground()
        }
        
        logMessage("Foreground monitoring started")
    }
    
    internal func stopForegroundMonitoring() {
        if let observer = foregroundObserver {
            NotificationCenter.default.removeObserver(observer)
            foregroundObserver = nil
        }
    }
    
    private func tryResumeAfterForeground() {
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.0) { [weak self] in
            guard let self = self else { return }
            
            guard OpenWearablesHealthSdkKeychain.isSyncActive(), self.hasAuth else { return }
            
            let fullDone = self.defaults.bool(forKey: self.fullDoneKey())
            guard self.hasResumableSyncSession() || !fullDone else { return }
            
            guard !self.isSyncInProgress else {
                self.logMessage("Sync already in progress after foreground")
                return
            }
            
            self.logMessage("App returned to foreground - resuming sync...")
            self.syncAll(fullExport: false) {
                self.logMessage("Foreground resume sync completed")
            }
        }
    }
    
    internal func markNetworkError() {
        wasDisconnected = true
    }
    
    private func tryResumeAfterNetworkRestored() {
        DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) { [weak self] in
            guard let self = self else { return }
            
            let fullDone = self.defaults.bool(forKey: self.fullDoneKey())
            guard self.hasResumableSyncSession() || !fullDone else {
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
