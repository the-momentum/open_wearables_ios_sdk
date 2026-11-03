import Foundation
import UIKit
import HealthKit
import BackgroundTasks

extension OpenWearablesHealthSDK {

    // MARK: - Background delivery
    internal func startBackgroundDelivery() {
        for q in activeObserverQueries { healthStore.stop(q) }
        activeObserverQueries.removeAll()

        let observableTypes = getQueryableTypes()

        for type in observableTypes {
            let observer = HKObserverQuery(sampleType: type, predicate: nil) { [weak self] _, completionHandler, error in
                guard let self = self else {
                    completionHandler()
                    return
                }

                if let error = error {
                    print("Observer error for \(type.identifier): \(error.localizedDescription)")
                    completionHandler()
                    return
                }

                self.triggerCombinedSync()
                completionHandler()
            }
            healthStore.execute(observer)
            activeObserverQueries.append(observer)
            healthStore.enableBackgroundDelivery(for: type, frequency: .immediate) { _, _ in }
        }
        logMessage("Background observers registered for \(observableTypes.count) types")
    }

    internal func stopBackgroundDelivery() {
        for q in activeObserverQueries { healthStore.stop(q) }
        activeObserverQueries.removeAll()
        
        let observableTypes = getQueryableTypes()
        
        for t in observableTypes {
            healthStore.disableBackgroundDelivery(for: t) { _, _ in }
        }
        logMessage("Background observers stopped")
    }

    // MARK: - BGTaskScheduler
    internal func scheduleAppRefresh() {
        guard #available(iOS 13.0, *) else { return }
        let req = BGAppRefreshTaskRequest(identifier: refreshTaskId)
        req.earliestBeginDate = Date(timeIntervalSinceNow: 15 * 60)
        do {
            try BGTaskScheduler.shared.submit(req)
            logMessage("Scheduled app refresh task")
        }
        catch {
            logMessage("scheduleAppRefresh error: \(error.localizedDescription)")
        }
    }

    internal func scheduleProcessing() {
        guard #available(iOS 13.0, *) else { return }
        let req = BGProcessingTaskRequest(identifier: processTaskId)
        req.requiresNetworkConnectivity = true
        req.requiresExternalPower = false
        do {
            try BGTaskScheduler.shared.submit(req)
            logMessage("Scheduled processing task")
        }
        catch {
            logMessage("scheduleProcessing error: \(error.localizedDescription)")
        }
    }

    internal func cancelAllBGTasks() {
        if #available(iOS 13.0, *) {
            BGTaskScheduler.shared.cancelAllTaskRequests()
            logMessage("Cancelled all background tasks")
        }
    }

    @available(iOS 13.0, *)
    internal func handleAppRefresh(task: BGAppRefreshTask) {
        scheduleAppRefresh()
        runSyncInBackgroundTask(task, name: "BGAppRefresh") { done in
            self.collectAllData(fullExport: false, isBackground: true, completion: done)
        }
    }

    @available(iOS 13.0, *)
    internal func handleProcessing(task: BGProcessingTask) {
        scheduleProcessing()
        retryOutboxIfPossible()
        runSyncInBackgroundTask(task, name: "BGProcessing") { done in
            self.collectAllData(fullExport: false, isBackground: true, completion: done)
        }
    }

    /// Completes `task` when the sync ends rather than after a fixed wait. The old 20s / 25s
    /// wait marked the task done while the round was still uploading, so iOS suspended the
    /// app mid-upload, and a processing task, which can run for minutes, used a fraction of
    /// its time. If iOS ends the task first, the expiration handler cancels the uploads and
    /// the round resumes on the next wake, since `SyncState` only advances on a 2xx.
    @available(iOS 13.0, *)
    private func runSyncInBackgroundTask(
        _ task: BGTask,
        name: String,
        sync: @escaping (@escaping () -> Void) -> Void
    ) {
        let lock = NSLock()
        var completed = false
        let complete: (Bool) -> Void = { success in
            lock.lock()
            let first = !completed
            completed = true
            lock.unlock()
            if first { task.setTaskCompleted(success: success) }
        }

        task.expirationHandler = { [weak self] in
            self?.logMessage("\(name) task expired - cancelling in-flight uploads")
            self?.cancelInFlightSyncUploads(reason: "backgroundExpiration")
            complete(false)
        }

        // iOS often runs these tasks overnight while the device is locked, when every
        // HealthKit read fails. Skip the round and leave it to the unlock, the next
        // foreground or a later task, which the flag tells to sync.
        guard isProtectedDataAvailableNow() else {
            logMessage("\(name): device locked - deferring sync until unlock")
            pendingSyncAfterUnlock = true
            complete(true)
            return
        }

        sync { complete(true) }
    }
}
