import Foundation
import HealthKit

extension OpenWearablesHealthSDK {

    // MARK: - Public API
    internal func serialize(samples: [HKSample], type: HKSampleType) -> [String: Any] {
        var records: [[String: Any]] = []

        for s in samples {
            if let w = s as? HKWorkout {
                records.append(_mapWorkout(w))
            } else if let q = s as? HKQuantitySample {
                records.append(_mapQuantity(q))
            } else if let c = s as? HKCategorySample {
                records.append(_mapCategory(c))
            } else if let corr = s as? HKCorrelation {
                records.append(contentsOf: _mapCorrelation(corr))
            } else {
                records.append([
                    "uuid": s.uuid.uuidString,
                    "type": s.sampleType.identifier,
                    "value": NSNull(),
                    "unit": NSNull(),
                    "startDate": ISO8601DateFormatter().string(from: s.startDate),
                    "endDate": ISO8601DateFormatter().string(from: s.endDate),
                    "source": _mapSource(s.sourceRevision, device: s.device),
                    "recordMetadata": _metadataList(s.metadata)
                ])
            }
        }

        return [
            "data": [
                "records": records
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
                            sleep.append(_mapCategoryEfficient(c, dateFormatter: dateFormatter))
                        } else {
                            records.append(_mapCategoryEfficient(c, dateFormatter: dateFormatter))
                        }
                    } else if let corr = s as? HKCorrelation {
                        records.append(contentsOf: _mapCorrelationEfficient(corr, dateFormatter: dateFormatter))
                    } else {
                        records.append([
                            "uuid": s.uuid.uuidString,
                            "type": s.sampleType.identifier,
                            "value": NSNull(),
                            "unit": NSNull(),
                            "startDate": dateFormatter.string(from: s.startDate),
                            "endDate": dateFormatter.string(from: s.endDate),
                            "source": _mapSource(s.sourceRevision, device: s.device),
                            "recordMetadata": _metadataList(s.metadata)
                        ])
                    }
                }
            }
        }
        
        return [
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
        
        for s in samples {
            if let w = s as? HKWorkout {
                workouts.append(_mapWorkout(w))
            } else if let q = s as? HKQuantitySample {
                records.append(_mapQuantity(q))
            } else if let c = s as? HKCategorySample {
                if c.categoryType.identifier == HKCategoryTypeIdentifier.sleepAnalysis.rawValue {
                    sleep.append(_mapCategory(c))
                } else {
                    records.append(_mapCategory(c))
                }
            } else if let corr = s as? HKCorrelation {
                records.append(contentsOf: _mapCorrelation(corr))
            } else {
                records.append([
                    "uuid": s.uuid.uuidString,
                    "type": s.sampleType.identifier,
                    "value": NSNull(),
                    "unit": NSNull(),
                    "startDate": ISO8601DateFormatter().string(from: s.startDate),
                    "endDate": ISO8601DateFormatter().string(from: s.endDate),
                    "source": _mapSource(s.sourceRevision, device: s.device),
                    "recordMetadata": _metadataList(s.metadata)
                ])
            }
        }
        
        return [
            "data": [
                "workouts": workouts,
                "records": records,
                "sleep": sleep
            ]
        ]
    }

    // MARK: - Type mapping
    internal func mapTypes(_ names: [String]) -> [HKSampleType] {
        var out: [HKSampleType] = []
        for n in names {
            switch n {
            case "steps":
                if let t = HKObjectType.quantityType(forIdentifier: .stepCount) { out.append(t) }
            case "distanceWalkingRunning":
                if let t = HKObjectType.quantityType(forIdentifier: .distanceWalkingRunning) { out.append(t) }
            case "distanceCycling":
                if let t = HKObjectType.quantityType(forIdentifier: .distanceCycling) { out.append(t) }
            case "flightsClimbed":
                if let t = HKObjectType.quantityType(forIdentifier: .flightsClimbed) { out.append(t) }
            case "walkingSpeed":
                if let t = HKObjectType.quantityType(forIdentifier: .walkingSpeed) { out.append(t) }
            case "walkingStepLength":
                if let t = HKObjectType.quantityType(forIdentifier: .walkingStepLength) { out.append(t) }
            case "walkingAsymmetryPercentage":
                if let t = HKObjectType.quantityType(forIdentifier: .walkingAsymmetryPercentage) { out.append(t) }
            case "walkingDoubleSupportPercentage":
                if let t = HKObjectType.quantityType(forIdentifier: .walkingDoubleSupportPercentage) { out.append(t) }
            case "sixMinuteWalkTestDistance":
                if let t = HKObjectType.quantityType(forIdentifier: .sixMinuteWalkTestDistance) { out.append(t) }
            case "activeEnergy":
                if let t = HKObjectType.quantityType(forIdentifier: .activeEnergyBurned) { out.append(t) }
            case "basalEnergy":
                if let t = HKObjectType.quantityType(forIdentifier: .basalEnergyBurned) { out.append(t) }
            case "heartRate":
                if let t = HKObjectType.quantityType(forIdentifier: .heartRate) { out.append(t) }
            case "restingHeartRate":
                if let t = HKObjectType.quantityType(forIdentifier: .restingHeartRate) { out.append(t) }
            case "heartRateVariabilitySDNN":
                if let t = HKObjectType.quantityType(forIdentifier: .heartRateVariabilitySDNN) { out.append(t) }
            case "vo2Max":
                if let t = HKObjectType.quantityType(forIdentifier: .vo2Max) { out.append(t) }
            case "oxygenSaturation":
                if let t = HKObjectType.quantityType(forIdentifier: .oxygenSaturation) { out.append(t) }
            case "respiratoryRate":
                if let t = HKObjectType.quantityType(forIdentifier: .respiratoryRate) { out.append(t) }
            case "bodyMass":
                if let t = HKObjectType.quantityType(forIdentifier: .bodyMass) { out.append(t) }
            case "height":
                if let t = HKObjectType.quantityType(forIdentifier: .height) { out.append(t) }
            case "bmi":
                if let t = HKObjectType.quantityType(forIdentifier: .bodyMassIndex) { out.append(t) }
            case "bodyFatPercentage":
                if let t = HKObjectType.quantityType(forIdentifier: .bodyFatPercentage) { out.append(t) }
            case "leanBodyMass":
                if let t = HKObjectType.quantityType(forIdentifier: .leanBodyMass) { out.append(t) }
            case "waistCircumference":
                if #available(iOS 16.0, *), let t = HKObjectType.quantityType(forIdentifier: .waistCircumference) { out.append(t) }
            case "bodyTemperature":
                if let t = HKObjectType.quantityType(forIdentifier: .bodyTemperature) { out.append(t) }
            case "bloodGlucose":
                if let t = HKObjectType.quantityType(forIdentifier: .bloodGlucose) { out.append(t) }
            case "insulinDelivery":
                if #available(iOS 16.0, *), let t = HKObjectType.quantityType(forIdentifier: .insulinDelivery) { out.append(t) }
            case "bloodPressureSystolic":
                if let t = HKObjectType.quantityType(forIdentifier: .bloodPressureSystolic) { out.append(t) }
            case "bloodPressureDiastolic":
                if let t = HKObjectType.quantityType(forIdentifier: .bloodPressureDiastolic) { out.append(t) }
            case "bloodPressure":
                if let t = HKObjectType.correlationType(forIdentifier: .bloodPressure) { out.append(t) }
            case "restingEnergy":
                if let t = HKObjectType.quantityType(forIdentifier: .basalEnergyBurned) { out.append(t) }
            case "bloodOxygen":
                if let t = HKObjectType.quantityType(forIdentifier: .oxygenSaturation) { out.append(t) }
            case "sleep":
                if let t = HKObjectType.categoryType(forIdentifier: .sleepAnalysis) { out.append(t) }
            case "mindfulSession":
                if let t = HKObjectType.categoryType(forIdentifier: .mindfulSession) { out.append(t) }
            case "menstrualFlow":
                if let t = HKObjectType.categoryType(forIdentifier: .menstrualFlow) { out.append(t) }
            case "cervicalMucusQuality":
                if let t = HKObjectType.categoryType(forIdentifier: .cervicalMucusQuality) { out.append(t) }
            case "ovulationTestResult":
                if let t = HKObjectType.categoryType(forIdentifier: .ovulationTestResult) { out.append(t) }
            case "sexualActivity":
                if let t = HKObjectType.categoryType(forIdentifier: .sexualActivity) { out.append(t) }
            case "dietaryEnergyConsumed":
                if let t = HKObjectType.quantityType(forIdentifier: .dietaryEnergyConsumed) { out.append(t) }
            case "dietaryCarbohydrates":
                if let t = HKObjectType.quantityType(forIdentifier: .dietaryCarbohydrates) { out.append(t) }
            case "dietaryProtein":
                if let t = HKObjectType.quantityType(forIdentifier: .dietaryProtein) { out.append(t) }
            case "dietaryFatTotal":
                if let t = HKObjectType.quantityType(forIdentifier: .dietaryFatTotal) { out.append(t) }
            case "dietaryWater":
                if let t = HKObjectType.quantityType(forIdentifier: .dietaryWater) { out.append(t) }
            case "workout":
                out.append(HKObjectType.workoutType())
            default:
                break
            }
        }
        return out
    }

    // MARK: - Record mappers

    private func _mapQuantity(_ q: HKQuantitySample) -> [String: Any] {
        let df = ISO8601DateFormatter()
        let (unit, unitOut) = _defaultUnit(for: q.quantityType)
        
        let value: Double
        let finalUnit: String
        
        if q.quantity.is(compatibleWith: unit) {
            value = q.quantity.doubleValue(for: unit)
            finalUnit = unitOut
        } else {
            let fallbackUnit = _getFallbackUnit(for: q.quantityType)
            value = q.quantity.doubleValue(for: fallbackUnit)
            finalUnit = fallbackUnit.unitString
        }

        return [
            "uuid": q.uuid.uuidString,
            "type": q.quantityType.identifier,
            "value": value,
            "unit": finalUnit,
            "startDate": df.string(from: q.startDate),
            "endDate": df.string(from: q.endDate),
            "source": _mapSource(q.sourceRevision, device: q.device),
            "recordMetadata": _metadataList(q.metadata)
        ]
    }

    private func _mapCategory(_ c: HKCategorySample) -> [String: Any] {
        let df = ISO8601DateFormatter()
        return [
            "uuid": c.uuid.uuidString,
            "type": c.categoryType.identifier,
            "value": c.value,
            "unit": NSNull(),
            "startDate": df.string(from: c.startDate),
            "endDate": df.string(from: c.endDate),
            "source": _mapSource(c.sourceRevision, device: c.device),
            "recordMetadata": _metadataList(c.metadata)
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
                    "uuid": q.uuid.uuidString,
                    "type": q.quantityType.identifier,
                    "value": value,
                    "unit": unitOut,
                    "startDate": df.string(from: q.startDate),
                    "endDate": df.string(from: q.endDate),
                    "source": source,
                    "recordMetadata": _metadataList(q.metadata)
                ])
            }
        }
        return records
    }

    private func _mapWorkout(_ w: HKWorkout) -> [String: Any] {
        let df = ISO8601DateFormatter()
        let stats = _buildWorkoutStats(w)

        return [
            "uuid": w.uuid.uuidString,
            "type": _workoutTypeString(w.workoutActivityType),
            "startDate": df.string(from: w.startDate),
            "endDate": df.string(from: w.endDate),
            "source": _mapSource(w.sourceRevision, device: w.device),
            "workoutStatistics": stats
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
        case .other: return "other"
        @unknown default: return "other"
        }
    }

    private func _metadataList(_ meta: [String: Any]?) -> [[String: Any]] {
        var list: [[String: Any]] = []
        guard let meta = meta else { return list }
        for (k, v) in meta {
            list.append([
                "key": k,
                "value": "\(v)"
            ])
        }
        return list
    }
    
    // MARK: - Memory-efficient mappers
    
    private func _mapQuantityEfficient(_ q: HKQuantitySample, dateFormatter: ISO8601DateFormatter) -> [String: Any] {
        let (unit, unitOut) = _defaultUnit(for: q.quantityType)
        
        let value: Double
        let finalUnit: String
        
        if q.quantity.is(compatibleWith: unit) {
            value = q.quantity.doubleValue(for: unit)
            finalUnit = unitOut
        } else {
            let fallbackUnit = _getFallbackUnit(for: q.quantityType)
            value = q.quantity.doubleValue(for: fallbackUnit)
            finalUnit = fallbackUnit.unitString
        }

        return [
            "uuid": q.uuid.uuidString,
            "type": q.quantityType.identifier,
            "value": value,
            "unit": finalUnit,
            "startDate": dateFormatter.string(from: q.startDate),
            "endDate": dateFormatter.string(from: q.endDate),
            "source": _mapSource(q.sourceRevision, device: q.device),
            "recordMetadata": _metadataList(q.metadata)
        ]
    }

    private func _mapCategoryEfficient(_ c: HKCategorySample, dateFormatter: ISO8601DateFormatter) -> [String: Any] {
        return [
            "uuid": c.uuid.uuidString,
            "type": c.categoryType.identifier,
            "value": c.value,
            "unit": NSNull(),
            "startDate": dateFormatter.string(from: c.startDate),
            "endDate": dateFormatter.string(from: c.endDate),
            "source": _mapSource(c.sourceRevision, device: c.device),
            "recordMetadata": _metadataList(c.metadata)
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
                    "uuid": q.uuid.uuidString,
                    "type": q.quantityType.identifier,
                    "value": value,
                    "unit": unitOut,
                    "startDate": dateFormatter.string(from: q.startDate),
                    "endDate": dateFormatter.string(from: q.endDate),
                    "source": source,
                    "recordMetadata": _metadataList(q.metadata)
                ])
            }
        }
        return records
    }

    private func _mapWorkoutEfficient(_ w: HKWorkout, dateFormatter: ISO8601DateFormatter) -> [String: Any] {
        let stats = _buildWorkoutStats(w)

        return [
            "uuid": w.uuid.uuidString,
            "type": _workoutTypeString(w.workoutActivityType),
            "startDate": dateFormatter.string(from: w.startDate),
            "endDate": dateFormatter.string(from: w.endDate),
            "source": _mapSource(w.sourceRevision, device: w.device),
            "workoutStatistics": stats
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
    
    // MARK: - Source mapper
    
    private func _mapSource(_ sourceRevision: HKSourceRevision, device: HKDevice?) -> [String: Any] {
        var result: [String: Any] = [:]
        
        result["name"] = sourceRevision.source.name
        result["bundleIdentifier"] = sourceRevision.source.bundleIdentifier
        
        if let version = sourceRevision.version { result["version"] = version }
        if let productType = sourceRevision.productType { result["productType"] = productType }
        
        let osVersion = sourceRevision.operatingSystemVersion
        result["operatingSystemVersion"] = [
            "majorVersion": osVersion.majorVersion,
            "minorVersion": osVersion.minorVersion,
            "patchVersion": osVersion.patchVersion
        ]
        
        if let device = device {
            if let name = device.name { result["deviceName"] = name }
            if let manufacturer = device.manufacturer { result["deviceManufacturer"] = manufacturer }
            if let model = device.model { result["deviceModel"] = model }
            if let hardwareVersion = device.hardwareVersion { result["deviceHardwareVersion"] = hardwareVersion }
            if let softwareVersion = device.softwareVersion { result["deviceSoftwareVersion"] = softwareVersion }
            if let firmwareVersion = device.firmwareVersion { result["deviceFirmwareVersion"] = firmwareVersion }
            if let localIdentifier = device.localIdentifier { result["deviceLocalIdentifier"] = localIdentifier }
            if let udiDeviceIdentifier = device.udiDeviceIdentifier { result["deviceUdiDeviceIdentifier"] = udiDeviceIdentifier }
        }
        
        return result
    }
}
