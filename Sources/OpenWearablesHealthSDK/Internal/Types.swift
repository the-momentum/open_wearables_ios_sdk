import Foundation
import HealthKit

// MARK: - Public Health Data Type Enum

/// Supported HealthKit data types for authorization and sync.
///
/// Use these values when calling `requestAuthorization(types:completion:)`.
///
/// ```swift
/// sdk.requestAuthorization(types: [.steps, .heartRate, .sleep]) { granted in
///     // ...
/// }
/// ```
public enum HealthDataType: String, CaseIterable, Sendable {
    // Activity & Mobility
    case steps
    case distanceWalkingRunning
    case distanceCycling
    case flightsClimbed
    case walkingSpeed
    case walkingStepLength
    case walkingAsymmetryPercentage
    case walkingDoubleSupportPercentage
    case sixMinuteWalkTestDistance
    case activeEnergy
    case basalEnergy
    
    // Heart & Cardiovascular
    case heartRate
    case restingHeartRate
    case heartRateVariabilitySDNN
    case vo2Max
    case oxygenSaturation
    case respiratoryRate
    
    // Body Measurements
    case bodyMass
    case height
    case bmi
    case bodyFatPercentage
    case leanBodyMass
    case waistCircumference
    case bodyTemperature
    
    // Blood & Metabolic
    case bloodGlucose
    case insulinDelivery
    case bloodPressureSystolic
    case bloodPressureDiastolic
    case bloodPressure
    
    // Sleep & Mindfulness
    case sleep
    case mindfulSession
    
    // Reproductive Health
    case menstrualFlow
    case cervicalMucusQuality
    case ovulationTestResult
    case sexualActivity
    
    // Nutrition
    case dietaryEnergyConsumed
    case dietaryCarbohydrates
    case dietaryProtein
    case dietaryFatTotal
    case dietaryWater
    
    // Workout
    case workout
    
    // Aliases (alternative names for the same underlying type)
    case restingEnergy
    case bloodOxygen
    
    /// Converts this health data type to the corresponding HealthKit sample type.
    /// Returns `nil` if the type is unavailable on the current OS version.
    public func toHKSampleType() -> HKSampleType? {
        switch self {
        case .steps:
            return HKObjectType.quantityType(forIdentifier: .stepCount)
        case .distanceWalkingRunning:
            return HKObjectType.quantityType(forIdentifier: .distanceWalkingRunning)
        case .distanceCycling:
            return HKObjectType.quantityType(forIdentifier: .distanceCycling)
        case .flightsClimbed:
            return HKObjectType.quantityType(forIdentifier: .flightsClimbed)
        case .walkingSpeed:
            return HKObjectType.quantityType(forIdentifier: .walkingSpeed)
        case .walkingStepLength:
            return HKObjectType.quantityType(forIdentifier: .walkingStepLength)
        case .walkingAsymmetryPercentage:
            return HKObjectType.quantityType(forIdentifier: .walkingAsymmetryPercentage)
        case .walkingDoubleSupportPercentage:
            return HKObjectType.quantityType(forIdentifier: .walkingDoubleSupportPercentage)
        case .sixMinuteWalkTestDistance:
            return HKObjectType.quantityType(forIdentifier: .sixMinuteWalkTestDistance)
        case .activeEnergy:
            return HKObjectType.quantityType(forIdentifier: .activeEnergyBurned)
        case .basalEnergy, .restingEnergy:
            return HKObjectType.quantityType(forIdentifier: .basalEnergyBurned)
        case .heartRate:
            return HKObjectType.quantityType(forIdentifier: .heartRate)
        case .restingHeartRate:
            return HKObjectType.quantityType(forIdentifier: .restingHeartRate)
        case .heartRateVariabilitySDNN:
            return HKObjectType.quantityType(forIdentifier: .heartRateVariabilitySDNN)
        case .vo2Max:
            return HKObjectType.quantityType(forIdentifier: .vo2Max)
        case .oxygenSaturation, .bloodOxygen:
            return HKObjectType.quantityType(forIdentifier: .oxygenSaturation)
        case .respiratoryRate:
            return HKObjectType.quantityType(forIdentifier: .respiratoryRate)
        case .bodyMass:
            return HKObjectType.quantityType(forIdentifier: .bodyMass)
        case .height:
            return HKObjectType.quantityType(forIdentifier: .height)
        case .bmi:
            return HKObjectType.quantityType(forIdentifier: .bodyMassIndex)
        case .bodyFatPercentage:
            return HKObjectType.quantityType(forIdentifier: .bodyFatPercentage)
        case .leanBodyMass:
            return HKObjectType.quantityType(forIdentifier: .leanBodyMass)
        case .waistCircumference:
            if #available(iOS 16.0, *) {
                return HKObjectType.quantityType(forIdentifier: .waistCircumference)
            }
            return nil
        case .bodyTemperature:
            return HKObjectType.quantityType(forIdentifier: .bodyTemperature)
        case .bloodGlucose:
            return HKObjectType.quantityType(forIdentifier: .bloodGlucose)
        case .insulinDelivery:
            if #available(iOS 16.0, *) {
                return HKObjectType.quantityType(forIdentifier: .insulinDelivery)
            }
            return nil
        case .bloodPressureSystolic:
            return HKObjectType.quantityType(forIdentifier: .bloodPressureSystolic)
        case .bloodPressureDiastolic:
            return HKObjectType.quantityType(forIdentifier: .bloodPressureDiastolic)
        case .bloodPressure:
            return HKObjectType.correlationType(forIdentifier: .bloodPressure)
        case .sleep:
            return HKObjectType.categoryType(forIdentifier: .sleepAnalysis)
        case .mindfulSession:
            return HKObjectType.categoryType(forIdentifier: .mindfulSession)
        case .menstrualFlow:
            return HKObjectType.categoryType(forIdentifier: .menstrualFlow)
        case .cervicalMucusQuality:
            return HKObjectType.categoryType(forIdentifier: .cervicalMucusQuality)
        case .ovulationTestResult:
            return HKObjectType.categoryType(forIdentifier: .ovulationTestResult)
        case .sexualActivity:
            return HKObjectType.categoryType(forIdentifier: .sexualActivity)
        case .dietaryEnergyConsumed:
            return HKObjectType.quantityType(forIdentifier: .dietaryEnergyConsumed)
        case .dietaryCarbohydrates:
            return HKObjectType.quantityType(forIdentifier: .dietaryCarbohydrates)
        case .dietaryProtein:
            return HKObjectType.quantityType(forIdentifier: .dietaryProtein)
        case .dietaryFatTotal:
            return HKObjectType.quantityType(forIdentifier: .dietaryFatTotal)
        case .dietaryWater:
            return HKObjectType.quantityType(forIdentifier: .dietaryWater)
        case .workout:
            return HKObjectType.workoutType()
        }
    }
}

extension OpenWearablesHealthSDK {

    // MARK: - Public API
    internal func serialize(samples: [HKSample], type: HKSampleType) -> [String: Any] {
        var workouts: [[String: Any]] = []
        var records: [[String: Any]] = []
        var sleep: [[String: Any]] = []
        let df = ISO8601DateFormatter()

        for s in samples {
            if let w = s as? HKWorkout {
                workouts.append(_mapWorkout(w))
            } else if let q = s as? HKQuantitySample {
                records.append(_mapQuantity(q))
            } else if let c = s as? HKCategorySample {
                if c.categoryType.identifier == HKCategoryTypeIdentifier.sleepAnalysis.rawValue {
                    sleep.append(_mapSleep(c))
                } else {
                    records.append(_mapCategory(c))
                }
            } else if let corr = s as? HKCorrelation {
                records.append(contentsOf: _mapCorrelation(corr))
            } else {
                records.append([
                    "id": s.uuid.uuidString,
                    "type": s.sampleType.identifier,
                    "startDate": df.string(from: s.startDate),
                    "endDate": df.string(from: s.endDate),
                    "zoneOffset": NSNull(),
                    "source": _mapSource(s.sourceRevision, device: s.device),
                    "value": NSNull(),
                    "unit": NSNull(),
                    "parentId": NSNull(),
                    "metadata": _metadataDict(s.metadata)
                ])
            }
        }

        return [
            "provider": "apple_health",
            "sdkVersion": OpenWearablesHealthSDK.sdkVersion,
            "syncTimestamp": df.string(from: Date()),
            "data": [
                "workouts": workouts,
                "records": records,
                "sleep": sleep
            ]
        ]
    }
    
    // MARK: - Memory-efficient streaming serialization
    internal func serializeCombinedStreaming(samples: [HKSample]) -> [String: Any] {
        var workouts: [[String: Any]] = []
        var records: [[String: Any]] = []
        var sleep: [[String: Any]] = []
        
        let dateFormatter = ISO8601DateFormatter()
        
        let batchSize = 100
        for batchStart in stride(from: 0, to: samples.count, by: batchSize) {
            autoreleasepool {
                let batchEnd = min(batchStart + batchSize, samples.count)
                let batch = samples[batchStart..<batchEnd]
                
                for s in batch {
                    if let w = s as? HKWorkout {
                        workouts.append(_mapWorkoutEfficient(w, dateFormatter: dateFormatter))
                    } else if let q = s as? HKQuantitySample {
                        records.append(_mapQuantityEfficient(q, dateFormatter: dateFormatter))
                    } else if let c = s as? HKCategorySample {
                        if c.categoryType.identifier == HKCategoryTypeIdentifier.sleepAnalysis.rawValue {
                            sleep.append(_mapSleepEfficient(c, dateFormatter: dateFormatter))
                        } else {
                            records.append(_mapCategoryEfficient(c, dateFormatter: dateFormatter))
                        }
                    } else if let corr = s as? HKCorrelation {
                        records.append(contentsOf: _mapCorrelationEfficient(corr, dateFormatter: dateFormatter))
                    } else {
                        records.append([
                            "id": s.uuid.uuidString,
                            "type": s.sampleType.identifier,
                            "startDate": dateFormatter.string(from: s.startDate),
                            "endDate": dateFormatter.string(from: s.endDate),
                            "zoneOffset": NSNull(),
                            "source": _mapSource(s.sourceRevision, device: s.device),
                            "value": NSNull(),
                            "unit": NSNull(),
                            "parentId": NSNull(),
                            "metadata": _metadataDict(s.metadata)
                        ])
                    }
                }
            }
        }
        
        return [
            "provider": "apple_health",
            "sdkVersion": OpenWearablesHealthSDK.sdkVersion,
            "syncTimestamp": dateFormatter.string(from: Date()),
            "data": [
                "workouts": workouts,
                "records": records,
                "sleep": sleep
            ]
        ]
    }
    
    // MARK: - Combined serialization (legacy)
    internal func serializeCombined(samples: [HKSample], anchors: [String: HKQueryAnchor]) -> [String: Any] {
        var workouts: [[String: Any]] = []
        var records: [[String: Any]] = []
        var sleep: [[String: Any]] = []
        let df = ISO8601DateFormatter()
        
        for s in samples {
            if let w = s as? HKWorkout {
                workouts.append(_mapWorkout(w))
            } else if let q = s as? HKQuantitySample {
                records.append(_mapQuantity(q))
            } else if let c = s as? HKCategorySample {
                if c.categoryType.identifier == HKCategoryTypeIdentifier.sleepAnalysis.rawValue {
                    sleep.append(_mapSleep(c))
                } else {
                    records.append(_mapCategory(c))
                }
            } else if let corr = s as? HKCorrelation {
                records.append(contentsOf: _mapCorrelation(corr))
            } else {
                records.append([
                    "id": s.uuid.uuidString,
                    "type": s.sampleType.identifier,
                    "startDate": df.string(from: s.startDate),
                    "endDate": df.string(from: s.endDate),
                    "zoneOffset": NSNull(),
                    "source": _mapSource(s.sourceRevision, device: s.device),
                    "value": NSNull(),
                    "unit": NSNull(),
                    "parentId": NSNull(),
                    "metadata": _metadataDict(s.metadata)
                ])
            }
        }
        
        return [
            "provider": "apple_health",
            "sdkVersion": OpenWearablesHealthSDK.sdkVersion,
            "syncTimestamp": df.string(from: Date()),
            "data": [
                "workouts": workouts,
                "records": records,
                "sleep": sleep
            ]
        ]
    }

    // MARK: - Type mapping
    
    internal func mapTypes(_ types: [HealthDataType]) -> [HKSampleType] {
        return types.compactMap { $0.toHKSampleType() }
    }
    
    /// Legacy mapping from raw strings - used for restoring persisted types from Keychain.
    internal func mapTypesFromStrings(_ names: [String]) -> [HKSampleType] {
        return names.compactMap { HealthDataType(rawValue: $0)?.toHKSampleType() }
    }

    // MARK: - Record mappers

    private func _mapQuantity(_ q: HKQuantitySample) -> [String: Any] {
        let df = ISO8601DateFormatter()
        let (unit, unitOut) = _defaultUnit(for: q.quantityType)
        
        var value: Double
        let finalUnit: String
        
        if q.quantity.is(compatibleWith: unit) {
            value = q.quantity.doubleValue(for: unit)
            finalUnit = unitOut
        } else {
            let fallbackUnit = _getFallbackUnit(for: q.quantityType)
            value = q.quantity.doubleValue(for: fallbackUnit)
            finalUnit = fallbackUnit.unitString
        }

        if q.quantityType.identifier == HKQuantityTypeIdentifier.oxygenSaturation.rawValue {
            value *= 100
        }

        return [
            "id": q.uuid.uuidString,
            "type": q.quantityType.identifier,
            "startDate": df.string(from: q.startDate),
            "endDate": df.string(from: q.endDate),
            "zoneOffset": NSNull(),
            "source": _mapSource(q.sourceRevision, device: q.device),
            "value": value,
            "unit": finalUnit,
            "parentId": NSNull(),
            "metadata": _metadataDict(q.metadata)
        ]
    }

    private func _mapCategory(_ c: HKCategorySample) -> [String: Any] {
        let df = ISO8601DateFormatter()
        return [
            "id": c.uuid.uuidString,
            "type": c.categoryType.identifier,
            "startDate": df.string(from: c.startDate),
            "endDate": df.string(from: c.endDate),
            "zoneOffset": NSNull(),
            "source": _mapSource(c.sourceRevision, device: c.device),
            "value": c.value,
            "unit": NSNull(),
            "parentId": NSNull(),
            "metadata": _metadataDict(c.metadata)
        ]
    }

    private func _mapSleep(_ c: HKCategorySample) -> [String: Any] {
        let df = ISO8601DateFormatter()
        return [
            "id": c.uuid.uuidString,
            "parentId": NSNull(),
            "stage": _sleepStageString(c.value),
            "startDate": df.string(from: c.startDate),
            "endDate": df.string(from: c.endDate),
            "zoneOffset": NSNull(),
            "source": _mapSource(c.sourceRevision, device: c.device),
            "values": NSNull(),
            "metadata": NSNull()
        ]
    }

    private func _mapCorrelation(_ corr: HKCorrelation) -> [[String: Any]] {
        var records: [[String: Any]] = []
        let df = ISO8601DateFormatter()
        let source = _mapSource(corr.sourceRevision, device: corr.device)

        for sample in corr.objects {
            if let q = sample as? HKQuantitySample {
                let (unit, unitOut) = _defaultUnit(for: q.quantityType)
                let value = q.quantity.doubleValue(for: unit)
                records.append([
                    "id": q.uuid.uuidString,
                    "type": q.quantityType.identifier,
                    "startDate": df.string(from: q.startDate),
                    "endDate": df.string(from: q.endDate),
                    "zoneOffset": NSNull(),
                    "source": source,
                    "value": value,
                    "unit": unitOut,
                    "parentId": NSNull(),
                    "metadata": _metadataDict(q.metadata)
                ])
            }
        }
        return records
    }

    private func _mapWorkout(_ w: HKWorkout) -> [String: Any] {
        let df = ISO8601DateFormatter()
        let stats = _buildWorkoutStats(w)

        return [
            "id": w.uuid.uuidString,
            "parentId": NSNull(),
            "type": _workoutTypeString(w.workoutActivityType),
            "startDate": df.string(from: w.startDate),
            "endDate": df.string(from: w.endDate),
            "zoneOffset": NSNull(),
            "source": _mapSource(w.sourceRevision, device: w.device),
            "title": NSNull(),
            "notes": NSNull(),
            "values": stats,
            "segments": NSNull(),
            "laps": NSNull(),
            "route": NSNull(),
            "samples": NSNull(),
            "metadata": NSNull()
        ]
    }

    // MARK: - Units / helpers

    private func _getFallbackUnit(for qt: HKQuantityType) -> HKUnit {
        switch qt {
        case HKObjectType.quantityType(forIdentifier: .stepCount):
            return .count()
        case HKObjectType.quantityType(forIdentifier: .heartRate),
             HKObjectType.quantityType(forIdentifier: .restingHeartRate):
            return .count().unitDivided(by: .minute())
        case HKObjectType.quantityType(forIdentifier: .distanceWalkingRunning),
             HKObjectType.quantityType(forIdentifier: .distanceCycling):
            return .meter()
        case HKObjectType.quantityType(forIdentifier: .bodyMass),
             HKObjectType.quantityType(forIdentifier: .height):
            return .meter()
        case HKObjectType.quantityType(forIdentifier: .bodyTemperature):
            return .degreeCelsius()
        case HKObjectType.quantityType(forIdentifier: .oxygenSaturation):
            return HKUnit.percent()
        case HKObjectType.quantityType(forIdentifier: .bloodGlucose):
            return HKUnit.gramUnit(with: .milli).unitDivided(by: HKUnit.literUnit(with: .deci))
        case HKObjectType.quantityType(forIdentifier: .bloodPressureSystolic),
             HKObjectType.quantityType(forIdentifier: .bloodPressureDiastolic):
            return HKUnit.millimeterOfMercury()
        default:
            return .count()
        }
    }

    private func _defaultUnit(for qt: HKQuantityType) -> (HKUnit, String) {
        switch qt {
        case HKObjectType.quantityType(forIdentifier: .stepCount):
            return (.count(), "count")
        case HKObjectType.quantityType(forIdentifier: .heartRate):
            return (.count().unitDivided(by: .minute()), "bpm")
        case HKObjectType.quantityType(forIdentifier: .restingHeartRate):
            return (.count().unitDivided(by: .minute()), "bpm")
        case HKObjectType.quantityType(forIdentifier: .heartRateVariabilitySDNN):
            return (.secondUnit(with: .milli), "ms")
        case HKObjectType.quantityType(forIdentifier: .basalEnergyBurned),
             HKObjectType.quantityType(forIdentifier: .activeEnergyBurned),
             HKObjectType.quantityType(forIdentifier: .dietaryEnergyConsumed):
            return (.kilocalorie(), "Cal")
        case HKObjectType.quantityType(forIdentifier: .distanceWalkingRunning),
             HKObjectType.quantityType(forIdentifier: .distanceCycling):
            return (.meter(), "m")
        case HKObjectType.quantityType(forIdentifier: .walkingSpeed):
            return (.meter().unitDivided(by: .second()), "m/s")
        case HKObjectType.quantityType(forIdentifier: .walkingStepLength):
            return (.meter(), "m")
        case HKObjectType.quantityType(forIdentifier: .walkingAsymmetryPercentage):
            return (HKUnit.percent(), "%")
        case HKObjectType.quantityType(forIdentifier: .walkingDoubleSupportPercentage):
            return (HKUnit.percent(), "%")
        case HKObjectType.quantityType(forIdentifier: .sixMinuteWalkTestDistance):
            return (.meter(), "m")
        case HKObjectType.quantityType(forIdentifier: .bodyMass):
            return (.gramUnit(with: .kilo), "kg")
        case HKObjectType.quantityType(forIdentifier: .height):
            return (.meter(), "m")
        case HKObjectType.quantityType(forIdentifier: .bodyMassIndex):
            let bmiUnit = HKUnit.gramUnit(with: .kilo).unitDivided(by: HKUnit.meter().unitMultiplied(by: HKUnit.meter()))
            return (bmiUnit, "kg/m²")
        case HKObjectType.quantityType(forIdentifier: .bodyFatPercentage):
            return (HKUnit.percent(), "%")
        case HKObjectType.quantityType(forIdentifier: .leanBodyMass):
            return (.gramUnit(with: .kilo), "kg")
        case HKObjectType.quantityType(forIdentifier: .waistCircumference):
            return (.meter(), "m")
        case HKObjectType.quantityType(forIdentifier: .bodyTemperature):
            return (.degreeCelsius(), "degC")
        case HKObjectType.quantityType(forIdentifier: .oxygenSaturation):
            return (HKUnit.percent(), "%")
        case HKObjectType.quantityType(forIdentifier: .respiratoryRate):
            return (.count().unitDivided(by: .minute()), "breaths/min")
        case HKObjectType.quantityType(forIdentifier: .bloodGlucose):
            let glucoseUnit = HKUnit.gramUnit(with: .milli).unitDivided(by: HKUnit.literUnit(with: .deci))
            return (glucoseUnit, "mg/dL")
        case HKObjectType.quantityType(forIdentifier: .insulinDelivery):
            return (HKUnit.internationalUnit(), "IU")
        case HKObjectType.quantityType(forIdentifier: .bloodPressureSystolic),
             HKObjectType.quantityType(forIdentifier: .bloodPressureDiastolic):
            return (HKUnit.millimeterOfMercury(), "mmHg")
        case HKObjectType.quantityType(forIdentifier: .vo2Max):
            let vo2Unit = HKUnit.literUnit(with: .milli).unitDivided(by: HKUnit.gramUnit(with: .kilo)).unitDivided(by: HKUnit.minute())
            return (vo2Unit, "mL/kg/min")
        case HKObjectType.quantityType(forIdentifier: .flightsClimbed):
            return (.count(), "count")
        case HKObjectType.quantityType(forIdentifier: .dietaryCarbohydrates),
             HKObjectType.quantityType(forIdentifier: .dietaryProtein),
             HKObjectType.quantityType(forIdentifier: .dietaryFatTotal):
            return (.gram(), "g")
        case HKObjectType.quantityType(forIdentifier: .dietaryWater):
            return (.liter(), "L")
        default:
            return (.count(), "count")
        }
    }

    internal func _workoutTypeString(_ t: HKWorkoutActivityType) -> String {
        switch t {
        case .running: return "running"
        case .walking: return "walking"
        case .hiking: return "hiking"
        case .wheelchairRunPace: return "wheelchair_run"
        case .wheelchairWalkPace: return "wheelchair_walk"
        case .cycling: return "cycling"
        case .handCycling: return "hand_cycling"
        case .swimming: return "swimming"
        case .paddleSports: return "paddle_sports"
        case .rowing: return "rowing"
        case .surfingSports: return "surfing"
        case .waterFitness: return "water_fitness"
        case .waterPolo: return "water_polo"
        case .waterSports: return "water_sports"
        case .traditionalStrengthTraining: return "strength_training"
        case .functionalStrengthTraining: return "functional_strength_training"
        case .coreTraining: return "core_training"
        case .crossTraining: return "cross_training"
        case .mixedCardio: return "mixed_cardio"
        case .highIntensityIntervalTraining: return "hiit"
        case .flexibility: return "flexibility"
        case .cooldown: return "cooldown"
        case .elliptical: return "elliptical"
        case .stairClimbing: return "stair_climbing"
        case .stairs: return "stairs"
        case .stepTraining: return "step_training"
        case .fitnessGaming: return "fitness_gaming"
        case .jumpRope: return "jump_rope"
        case .pilates: return "pilates"
        case .preparationAndRecovery: return "preparation_and_recovery"
        case .yoga: return "yoga"
        case .mindAndBody: return "mind_and_body"
        case .barre: return "barre"
        case .taiChi: return "tai_chi"
        case .dance: return "dance"
        case .danceInspiredTraining: return "dance_inspired_training"
        case .socialDance: return "social_dance"
        case .cardioDance: return "cardio_dance"
        case .tennis: return "tennis"
        case .tableTennis: return "table_tennis"
        case .badminton: return "badminton"
        case .squash: return "squash"
        case .racquetball: return "racquetball"
        case .pickleball: return "pickleball"
        case .soccer: return "soccer"
        case .basketball: return "basketball"
        case .baseball: return "baseball"
        case .softball: return "softball"
        case .americanFootball: return "american_football"
        case .australianFootball: return "australian_football"
        case .rugby: return "rugby"
        case .hockey: return "hockey"
        case .lacrosse: return "lacrosse"
        case .volleyball: return "volleyball"
        case .handball: return "handball"
        case .cricket: return "cricket"
        case .discSports: return "disc_sports"
        case .boxing: return "boxing"
        case .kickboxing: return "kickboxing"
        case .martialArts: return "martial_arts"
        case .wrestling: return "wrestling"
        case .fencing: return "fencing"
        case .snowSports: return "snow_sports"
        case .crossCountrySkiing: return "cross_country_skiing"
        case .downhillSkiing: return "downhill_skiing"
        case .snowboarding: return "snowboarding"
        case .skatingSports: return "skating"
        case .curling: return "curling"
        case .golf: return "golf"
        case .archery: return "archery"
        case .fishing: return "fishing"
        case .hunting: return "hunting"
        case .climbing: return "climbing"
        case .equestrianSports: return "equestrian"
        case .play: return "play"
        case .trackAndField: return "track_and_field"
        case .bowling: return "bowling"
        case .gymnastics: return "gymnastics"
        case .mixedMetabolicCardioTraining: return "mixed_metabolic_cardio_training"
        case .sailing: return "sailing"
        case .swimBikeRun: return "swim_bike_run"
        case .transition: return "transition"
        case .underwaterDiving: return "underwater_diving"
        case .other: return "other"
        @unknown default: return "other"
        }
    }

    
    // MARK: - Memory-efficient mappers
    
    private func _mapQuantityEfficient(_ q: HKQuantitySample, dateFormatter: ISO8601DateFormatter) -> [String: Any] {
        let (unit, unitOut) = _defaultUnit(for: q.quantityType)
        
        var value: Double
        let finalUnit: String
        
        if q.quantity.is(compatibleWith: unit) {
            value = q.quantity.doubleValue(for: unit)
            finalUnit = unitOut
        } else {
            let fallbackUnit = _getFallbackUnit(for: q.quantityType)
            value = q.quantity.doubleValue(for: fallbackUnit)
            finalUnit = fallbackUnit.unitString
        }

        if q.quantityType.identifier == HKQuantityTypeIdentifier.oxygenSaturation.rawValue {
            value *= 100
        }

        return [
            "id": q.uuid.uuidString,
            "type": q.quantityType.identifier,
            "startDate": dateFormatter.string(from: q.startDate),
            "endDate": dateFormatter.string(from: q.endDate),
            "zoneOffset": NSNull(),
            "source": _mapSource(q.sourceRevision, device: q.device),
            "value": value,
            "unit": finalUnit,
            "parentId": NSNull(),
            "metadata": _metadataDict(q.metadata)
        ]
    }

    private func _mapCategoryEfficient(_ c: HKCategorySample, dateFormatter: ISO8601DateFormatter) -> [String: Any] {
        return [
            "id": c.uuid.uuidString,
            "type": c.categoryType.identifier,
            "startDate": dateFormatter.string(from: c.startDate),
            "endDate": dateFormatter.string(from: c.endDate),
            "zoneOffset": NSNull(),
            "source": _mapSource(c.sourceRevision, device: c.device),
            "value": c.value,
            "unit": NSNull(),
            "parentId": NSNull(),
            "metadata": _metadataDict(c.metadata)
        ]
    }

    private func _mapSleepEfficient(_ c: HKCategorySample, dateFormatter: ISO8601DateFormatter) -> [String: Any] {
        return [
            "id": c.uuid.uuidString,
            "parentId": NSNull(),
            "stage": _sleepStageString(c.value),
            "startDate": dateFormatter.string(from: c.startDate),
            "endDate": dateFormatter.string(from: c.endDate),
            "zoneOffset": NSNull(),
            "source": _mapSource(c.sourceRevision, device: c.device),
            "values": NSNull(),
            "metadata": NSNull()
        ]
    }

    private func _mapCorrelationEfficient(_ corr: HKCorrelation, dateFormatter: ISO8601DateFormatter) -> [[String: Any]] {
        var records: [[String: Any]] = []
        let source = _mapSource(corr.sourceRevision, device: corr.device)

        for sample in corr.objects {
            if let q = sample as? HKQuantitySample {
                let (unit, unitOut) = _defaultUnit(for: q.quantityType)
                let value = q.quantity.doubleValue(for: unit)
                records.append([
                    "id": q.uuid.uuidString,
                    "type": q.quantityType.identifier,
                    "startDate": dateFormatter.string(from: q.startDate),
                    "endDate": dateFormatter.string(from: q.endDate),
                    "zoneOffset": NSNull(),
                    "source": source,
                    "value": value,
                    "unit": unitOut,
                    "parentId": NSNull(),
                    "metadata": _metadataDict(q.metadata)
                ])
            }
        }
        return records
    }

    private func _mapWorkoutEfficient(_ w: HKWorkout, dateFormatter: ISO8601DateFormatter) -> [String: Any] {
        let stats = _buildWorkoutStats(w)

        return [
            "id": w.uuid.uuidString,
            "parentId": NSNull(),
            "type": _workoutTypeString(w.workoutActivityType),
            "startDate": dateFormatter.string(from: w.startDate),
            "endDate": dateFormatter.string(from: w.endDate),
            "zoneOffset": NSNull(),
            "source": _mapSource(w.sourceRevision, device: w.device),
            "title": NSNull(),
            "notes": NSNull(),
            "values": stats,
            "segments": NSNull(),
            "laps": NSNull(),
            "route": NSNull(),
            "samples": NSNull(),
            "metadata": NSNull()
        ]
    }
    
    // MARK: - Workout stats builder (shared between mappers)
    
    private func _buildWorkoutStats(_ w: HKWorkout) -> [[String: Any]] {
        var stats: [[String: Any]] = []
        
        stats.append([
            "type": "duration",
            "value": w.duration,
            "unit": "s"
        ])
        
        if #available(iOS 16.0, *) {
            if let energyType = HKQuantityType.quantityType(forIdentifier: .activeEnergyBurned),
               let energyStats = w.statistics(for: energyType),
               let sum = energyStats.sumQuantity() {
                stats.append(["type": "activeEnergyBurned", "value": sum.doubleValue(for: .kilocalorie()), "unit": "kcal"])
            }
            if let basalType = HKQuantityType.quantityType(forIdentifier: .basalEnergyBurned),
               let basalStats = w.statistics(for: basalType),
               let sum = basalStats.sumQuantity() {
                stats.append(["type": "basalEnergyBurned", "value": sum.doubleValue(for: .kilocalorie()), "unit": "kcal"])
            }
            let distanceTypes: [HKQuantityTypeIdentifier] = [.distanceWalkingRunning, .distanceCycling, .distanceSwimming, .distanceDownhillSnowSports]
            for distanceTypeId in distanceTypes {
                if let distType = HKQuantityType.quantityType(forIdentifier: distanceTypeId),
                   let distStats = w.statistics(for: distType),
                   let sum = distStats.sumQuantity() {
                    stats.append(["type": "distance", "value": sum.doubleValue(for: .meter()), "unit": "m"])
                    break
                }
            }
            if let stepType = HKQuantityType.quantityType(forIdentifier: .stepCount),
               let stepStats = w.statistics(for: stepType),
               let sum = stepStats.sumQuantity() {
                stats.append(["type": "stepCount", "value": sum.doubleValue(for: .count()), "unit": "count"])
            }
            if let strokeType = HKQuantityType.quantityType(forIdentifier: .swimmingStrokeCount),
               let strokeStats = w.statistics(for: strokeType),
               let sum = strokeStats.sumQuantity() {
                stats.append(["type": "swimmingStrokeCount", "value": sum.doubleValue(for: .count()), "unit": "count"])
            }
            if let hrType = HKQuantityType.quantityType(forIdentifier: .heartRate),
               let hrStats = w.statistics(for: hrType) {
                let bpmUnit = HKUnit.count().unitDivided(by: .minute())
                if let min = hrStats.minimumQuantity() { stats.append(["type": "minHeartRate", "value": min.doubleValue(for: bpmUnit), "unit": "bpm"]) }
                if let avg = hrStats.averageQuantity() { stats.append(["type": "averageHeartRate", "value": avg.doubleValue(for: bpmUnit), "unit": "bpm"]) }
                if let max = hrStats.maximumQuantity() { stats.append(["type": "maxHeartRate", "value": max.doubleValue(for: bpmUnit), "unit": "bpm"]) }
            }
            if let powerType = HKQuantityType.quantityType(forIdentifier: .runningPower),
               let powerStats = w.statistics(for: powerType),
               let avg = powerStats.averageQuantity() {
                stats.append(["type": "averageRunningPower", "value": avg.doubleValue(for: .watt()), "unit": "W"])
            }
            if let speedType = HKQuantityType.quantityType(forIdentifier: .runningSpeed),
               let speedStats = w.statistics(for: speedType),
               let avg = speedStats.averageQuantity() {
                stats.append(["type": "averageRunningSpeed", "value": avg.doubleValue(for: HKUnit.meter().unitDivided(by: .second())), "unit": "m/s"])
            }
            if let strideType = HKQuantityType.quantityType(forIdentifier: .runningStrideLength),
               let strideStats = w.statistics(for: strideType),
               let avg = strideStats.averageQuantity() {
                stats.append(["type": "averageRunningStrideLength", "value": avg.doubleValue(for: .meter()), "unit": "m"])
            }
            if let oscType = HKQuantityType.quantityType(forIdentifier: .runningVerticalOscillation),
               let oscStats = w.statistics(for: oscType),
               let avg = oscStats.averageQuantity() {
                stats.append(["type": "averageVerticalOscillation", "value": avg.doubleValue(for: .meterUnit(with: .centi)), "unit": "cm"])
            }
            if let gctType = HKQuantityType.quantityType(forIdentifier: .runningGroundContactTime),
               let gctStats = w.statistics(for: gctType),
               let avg = gctStats.averageQuantity() {
                stats.append(["type": "averageGroundContactTime", "value": avg.doubleValue(for: .secondUnit(with: .milli)), "unit": "ms"])
            }
        } else {
            if let energy = w.totalEnergyBurned {
                stats.append(["type": "activeEnergyBurned", "value": energy.doubleValue(for: .kilocalorie()), "unit": "kcal"])
            }
            if let dist = w.totalDistance {
                stats.append(["type": "distance", "value": dist.doubleValue(for: .meter()), "unit": "m"])
            }
        }
        
        // Metadata-based statistics
        if let elevationAscended = w.metadata?[HKMetadataKeyElevationAscended] as? HKQuantity { stats.append(["type": "elevationAscended", "value": elevationAscended.doubleValue(for: .meter()), "unit": "m"]) }
        if let elevationDescended = w.metadata?[HKMetadataKeyElevationDescended] as? HKQuantity { stats.append(["type": "elevationDescended", "value": elevationDescended.doubleValue(for: .meter()), "unit": "m"]) }
        if let avgSpeed = w.metadata?[HKMetadataKeyAverageSpeed] as? HKQuantity { stats.append(["type": "averageSpeed", "value": avgSpeed.doubleValue(for: HKUnit.meter().unitDivided(by: .second())), "unit": "m/s"]) }
        if let maxSpeed = w.metadata?[HKMetadataKeyMaximumSpeed] as? HKQuantity { stats.append(["type": "maxSpeed", "value": maxSpeed.doubleValue(for: HKUnit.meter().unitDivided(by: .second())), "unit": "m/s"]) }
        if let avgMETs = w.metadata?[HKMetadataKeyAverageMETs] as? HKQuantity { stats.append(["type": "averageMETs", "value": avgMETs.doubleValue(for: HKUnit.kilocalorie().unitDivided(by: HKUnit.gramUnit(with: .kilo).unitMultiplied(by: .hour()))), "unit": "kcal/kg/hr"]) }
        if let lapLength = w.metadata?[HKMetadataKeyLapLength] as? HKQuantity { stats.append(["type": "lapLength", "value": lapLength.doubleValue(for: .meter()), "unit": "m"]) }
        if let swimmingLocationType = w.metadata?[HKMetadataKeySwimmingLocationType] as? NSNumber { stats.append(["type": "swimmingLocationType", "value": swimmingLocationType.intValue, "unit": "enum"]) }
        if let indoorWorkout = w.metadata?[HKMetadataKeyIndoorWorkout] as? Bool { stats.append(["type": "indoorWorkout", "value": indoorWorkout ? 1 : 0, "unit": "bool"]) }
        if let weatherTemp = w.metadata?[HKMetadataKeyWeatherTemperature] as? HKQuantity { stats.append(["type": "weatherTemperature", "value": weatherTemp.doubleValue(for: .degreeCelsius()), "unit": "degC"]) }
        if let weatherHumidity = w.metadata?[HKMetadataKeyWeatherHumidity] as? HKQuantity { stats.append(["type": "weatherHumidity", "value": weatherHumidity.doubleValue(for: .percent()), "unit": "%"]) }
        
        return stats
    }
    
    // MARK: - Source mapper (unified format)
    
    private func _mapSource(_ sourceRevision: HKSourceRevision, device: HKDevice?) -> [String: Any] {
        var result: [String: Any] = [
            "appId": sourceRevision.source.bundleIdentifier,
            "deviceId": NSNull(),
            "deviceName": (device?.name) as Any? ?? NSNull(),
            "deviceManufacturer": (device?.manufacturer) as Any? ?? NSNull(),
            "deviceModel": (sourceRevision.productType) as Any? ?? NSNull(),
            "deviceType": _inferDeviceType(productType: sourceRevision.productType, device: device),
            "recordingMethod": NSNull()
        ]

        if let hw = device?.hardwareVersion { result["deviceHardwareVersion"] = hw }
        if let sw = device?.softwareVersion { result["deviceSoftwareVersion"] = sw }

        let osVersion = sourceRevision.operatingSystemVersion
        result["operatingSystemVersion"] = [
            "majorVersion": osVersion.majorVersion,
            "minorVersion": osVersion.minorVersion,
            "patchVersion": osVersion.patchVersion
        ]

        return result
    }
    
    private func _inferDeviceType(productType: String?, device: HKDevice?) -> Any {
        if let pt = productType?.lowercased() {
            if pt.contains("watch") { return "watch" }
            if pt.contains("iphone") { return "phone" }
            if pt.contains("ipad") { return "phone" }
        }
        if let name = device?.name?.lowercased() {
            if name.contains("watch") { return "watch" }
            if name.contains("iphone") { return "phone" }
        }
        return NSNull()
    }
    
    // MARK: - Metadata (unified: dict or null)
    
    private func _metadataDict(_ meta: [String: Any]?) -> Any {
        guard let meta = meta, !meta.isEmpty else { return NSNull() }
        var result: [String: Any] = [:]
        for (k, v) in meta {
            result[k] = "\(v)"
        }
        return result
    }
    
    // MARK: - Sleep stage mapping
    
    private func _sleepStageString(_ value: Int) -> String {
        switch value {
        case 0: return "in_bed"
        case 1: return "sleeping"
        case 2: return "awake"
        case 3: return "light"
        case 4: return "deep"
        case 5: return "rem"
        default: return "unknown"
        }
    }
}
