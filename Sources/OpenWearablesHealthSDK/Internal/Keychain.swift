import Foundation
import Security

/// Secure storage for credentials using iOS Keychain
internal class OpenWearablesHealthSdkKeychain {
    
    private static let service = "com.openwearables.healthsdk.tokens"
    private static let defaults = UserDefaults(suiteName: "com.openwearables.healthsdk.config") ?? .standard
    
    // MARK: - Keys
    private static let accessTokenKey = "accessToken"
    private static let refreshTokenKey = "refreshToken"
    private static let userIdKey = "userId"
    private static let apiKeyKey = "apiKey"
    private static let baseUrlKey = "baseUrl"
    private static let hostKey = "host"
    private static let customSyncUrlKey = "customSyncUrl"
    private static let customRefreshUrlKey = "customRefreshUrl"
    private static let syncActiveKey = "syncActive"
    private static let trackedTypesKey = "trackedTypes"
    private static let appInstalledKey = "appInstalled"
    private static let syncDaysBackKey = "syncDaysBack"
    
    // MARK: - Fresh Install Detection
    
    /// Call this on app launch to clear Keychain if app was reinstalled.
    /// UserDefaults is cleared on uninstall, but Keychain persists.
    /// If UserDefaults flag is missing but Keychain has data, the app was reinstalled.
    static func clearKeychainIfReinstalled() {
        let wasInstalled = defaults.bool(forKey: appInstalledKey)
        
        if !wasInstalled {
            if hasSession() {
                NSLog("[OpenWearablesHealthSDK] App reinstalled - clearing stale Keychain data")
                clearAll()
            }
            
            defaults.set(true, forKey: appInstalledKey)
            defaults.synchronize()
        }
    }
    
    // MARK: - Save Credentials
    
    static func saveCredentials(userId: String, accessToken: String? = nil, refreshToken: String? = nil) {
        save(key: userIdKey, value: userId)
        if let accessToken = accessToken {
            save(key: accessTokenKey, value: accessToken)
        }
        if let refreshToken = refreshToken {
            save(key: refreshTokenKey, value: refreshToken)
        }
    }
    
    // MARK: - Load Credentials
    
    static func getAccessToken() -> String? {
        return load(key: accessTokenKey)
    }
    
    static func getRefreshToken() -> String? {
        return load(key: refreshTokenKey)
    }
    
    static func getUserId() -> String? {
        return load(key: userIdKey)
    }
    
    // MARK: - Update Tokens (after refresh)
    
    static func updateTokens(accessToken: String, refreshToken: String?) {
        save(key: accessTokenKey, value: accessToken)
        if let refreshToken = refreshToken {
            save(key: refreshTokenKey, value: refreshToken)
        }
    }
    
    static func hasSession() -> Bool {
        guard getUserId() != nil else { return false }
        return getAccessToken() != nil || getApiKey() != nil
    }
    
    // MARK: - Host (stored in UserDefaults, not sensitive)
    
    static func saveHost(_ host: String?) {
        if let host = host {
            defaults.set(host, forKey: hostKey)
        } else {
            defaults.removeObject(forKey: hostKey)
        }
        defaults.synchronize()
    }
    
    static func getHost() -> String? {
        return defaults.string(forKey: hostKey)
    }
    
    // Legacy - kept for migration/cleanup
    static func saveCustomSyncUrl(_ url: String?) {
        if let url = url {
            defaults.set(url, forKey: customSyncUrlKey)
        } else {
            defaults.removeObject(forKey: customSyncUrlKey)
        }
        defaults.synchronize()
    }
    
    static func getCustomSyncUrl() -> String? {
        return defaults.string(forKey: customSyncUrlKey)
    }

    /// Optional override for the token-refresh endpoint. When unset, the SDK
    /// refreshes against "{apiBaseUrl}/token/refresh". Used by deployments whose
    /// auth/mint server differs from the data-sync host. Persisted in the shared
    /// suite so background refresh tasks (run in a fresh process) can read it.
    static func saveCustomRefreshUrl(_ url: String?) {
        if let url = url, !url.isEmpty {
            defaults.set(url, forKey: customRefreshUrlKey)
        } else {
            defaults.removeObject(forKey: customRefreshUrlKey)
        }
        defaults.synchronize()
    }

    static func getCustomRefreshUrl() -> String? {
        return defaults.string(forKey: customRefreshUrlKey)
    }
    
    // MARK: - Sync Active State
    
    static func setSyncActive(_ active: Bool) {
        defaults.set(active, forKey: syncActiveKey)
        defaults.synchronize()
    }
    
    static func isSyncActive() -> Bool {
        return defaults.bool(forKey: syncActiveKey)
    }
    
    // MARK: - Tracked Types
    
    static func saveTrackedTypes(_ types: [String]) {
        defaults.set(types, forKey: trackedTypesKey)
        defaults.synchronize()
    }
    
    static func getTrackedTypes() -> [String]? {
        return defaults.stringArray(forKey: trackedTypesKey)
    }
    
    // MARK: - Sync Days Back
    
    static func saveSyncDaysBack(_ days: Int) {
        defaults.set(days, forKey: syncDaysBackKey)
        defaults.synchronize()
    }
    
    /// Returns the stored sync days back, or 0 if not set (meaning full sync / no limit).
    static func getSyncDaysBack() -> Int {
        return defaults.integer(forKey: syncDaysBackKey)
    }
    
    // MARK: - API Key (alternative auth mode)
    
    static func saveApiKey(_ apiKey: String) {
        save(key: apiKeyKey, value: apiKey)
    }
    
    static func getApiKey() -> String? {
        return load(key: apiKeyKey)
    }
    
    // MARK: - Clear
    
    static func clearAll() {
        delete(key: accessTokenKey)
        delete(key: refreshTokenKey)
        delete(key: userIdKey)
        delete(key: apiKeyKey)
        defaults.removeObject(forKey: hostKey)
        defaults.removeObject(forKey: customSyncUrlKey)
        defaults.removeObject(forKey: customRefreshUrlKey)
        defaults.removeObject(forKey: syncActiveKey)
        defaults.removeObject(forKey: trackedTypesKey)
        defaults.removeObject(forKey: syncDaysBackKey)
        defaults.synchronize()
    }
    
    // MARK: - Private Keychain Operations
    
    private static func save(key: String, value: String) {
        guard let data = value.data(using: .utf8) else { return }
        
        delete(key: key)
        
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
            kSecValueData as String: data,
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        ]
        
        let status = SecItemAdd(query as CFDictionary, nil)
        if status != errSecSuccess {
            NSLog("[OpenWearablesHealthSDK] Keychain save failed for \(key): \(status)")
        }
    }
    
    private static func load(key: String) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne
        ]
        
        var result: AnyObject?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        
        guard status == errSecSuccess,
              let data = result as? Data,
              let string = String(data: data, encoding: .utf8) else {
            return nil
        }
        
        return string
    }
    
    private static func delete(key: String) {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: key
        ]
        
        SecItemDelete(query as CFDictionary)
    }
}
