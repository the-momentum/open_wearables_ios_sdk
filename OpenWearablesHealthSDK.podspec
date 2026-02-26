Pod::Spec.new do |s|
  s.name         = 'OpenWearablesHealthSDK'
  s.version      = '0.4.0'
  s.summary      = 'iOS SDK for background health data synchronization to the Open Wearables platform.'
  s.description  = <<-DESC
    Native iOS SDK for secure background health data synchronization from Apple HealthKit
    to the Open Wearables platform. Supports streaming sync, resumable sessions,
    dual authentication (tokens + API key), background execution, and automatic retries.
  DESC
  s.homepage     = 'https://github.com/kmlpiekarz/open_wearables_ios_sdk'
  s.license      = { :type => 'MIT', :file => 'LICENSE' }
  s.author       = { 'Open Wearables' => 'hello@openwearables.io' }
  s.source       = { :git => 'https://github.com/kmlpiekarz/open_wearables_ios_sdk.git', :tag => s.version.to_s }
  s.platform     = :ios, '14.0'
  s.swift_version = '5.0'
  s.source_files = 'Sources/OpenWearablesHealthSDK/**/*.swift'
  s.frameworks   = 'HealthKit', 'BackgroundTasks', 'UIKit'
end
