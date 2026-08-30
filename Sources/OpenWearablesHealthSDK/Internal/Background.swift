import BackgroundTasks
import Foundation
import HealthKit
import UIKit

@available(iOS 13.0, *)
private final class BackgroundTaskFinisher {
    private let task: BGTask
    private let lock = NSLock()
    private var finished = false

    init(task: BGTask) {
        self.task = task
    }

    func finish(success: Bool) {
        lock.lock()
        guard !finished else {
            lock.unlock()
            return
        }
        finished = true
        lock.unlock()
        task.setTaskCompleted(success: success)
    }
}

extension OpenWearablesHealthSDK {
    // MARK: - Background delivery

    func startBackgroundDelivery() {
        for q in activeObserverQueries {
            healthStore.stop(q)
        }
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

                self.triggerCombinedSync(observerCompletion: completionHandler)
            }
            healthStore.execute(observer)
            activeObserverQueries.append(observer)
            healthStore.enableBackgroundDelivery(for: type, frequency: .immediate) { _, _ in }
        }
        logMessage("Background observers registered for \(observableTypes.count) types")
    }

    func stopBackgroundDelivery() {
        finishObserverCompletions()
        for q in activeObserverQueries {
            healthStore.stop(q)
        }
        activeObserverQueries.removeAll()

        let observableTypes = getQueryableTypes()

        for t in observableTypes {
            healthStore.disableBackgroundDelivery(for: t) { _, _ in }
        }
        logMessage("Background observers stopped")
    }

    // MARK: - BGTaskScheduler

    func scheduleAppRefresh() {
        guard #available(iOS 13.0, *) else { return }
        let req = BGAppRefreshTaskRequest(identifier: refreshTaskId)
        req.earliestBeginDate = Date(timeIntervalSinceNow: 15 * 60)
        do {
            try BGTaskScheduler.shared.submit(req)
            logMessage("Scheduled app refresh task")
        } catch {
            logMessage("scheduleAppRefresh error: \(error.localizedDescription)")
        }
    }

    func scheduleProcessing() {
        guard #available(iOS 13.0, *) else { return }
        let req = BGProcessingTaskRequest(identifier: processTaskId)
        req.requiresNetworkConnectivity = true
        req.requiresExternalPower = false
        do {
            try BGTaskScheduler.shared.submit(req)
            logMessage("Scheduled processing task")
        } catch {
            logMessage("scheduleProcessing error: \(error.localizedDescription)")
        }
    }

    func cancelAllBGTasks() {
        if #available(iOS 13.0, *) {
            BGTaskScheduler.shared.cancelAllTaskRequests()
            logMessage("Cancelled all background tasks")
        }
    }

    @available(iOS 13.0, *)
    func handleAppRefresh(task: BGAppRefreshTask) {
        scheduleAppRefresh()
        let finisher = BackgroundTaskFinisher(task: task)

        task.expirationHandler = { [weak self] in
            self?.logMessage("BGAppRefresh task expired - cancelling in-flight uploads")
            self?.cancelInFlightForegroundUploads()
            self?.finishObserverCompletions()
            finisher.finish(success: false)
        }

        collectAllData(fullExport: false, isBackground: true) {
            finisher.finish(success: true)
        }
    }

    @available(iOS 13.0, *)
    func handleProcessing(task: BGProcessingTask) {
        scheduleProcessing()
        let finisher = BackgroundTaskFinisher(task: task)

        task.expirationHandler = { [weak self] in
            self?.logMessage("BGProcessing task expired - cancelling in-flight uploads")
            self?.cancelInFlightForegroundUploads()
            self?.finishObserverCompletions()
            finisher.finish(success: false)
        }

        retryOutboxIfPossible()
        collectAllData(fullExport: false, isBackground: true) {
            finisher.finish(success: true)
        }
    }
}
