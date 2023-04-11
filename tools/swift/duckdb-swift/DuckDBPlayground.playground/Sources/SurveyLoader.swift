
import Foundation
import PlaygroundSupport
import System

public enum SurveyLoader {
  
  enum Failure {
    case unableToDownloadSurveyContent
  }
  
  private static let localDataURL = playgroundSharedDataDirectory
    .appending(component: "org.duckdb", directoryHint: .isDirectory)
    .appending(component: "surveys")
  
  public static func downloadSurveyCSV(forYear year: Int) async throws -> URL {
    try await downloadSurveyIfNeeded(forYear: year)
    return localSurveyDataURL(forYear: year)
      .appending(component: "survey_results_public.csv")
  }
  
  public static func downloadSurveySchemaCSV(forYear year: Int) async throws -> URL {
    try await downloadSurveyIfNeeded(forYear: year)
    return localSurveyDataURL(forYear: year)
      .appending(component: "survey_results_schema.csv")
  }
  
  public static func downloadSurveyIfNeeded(forYear year: Int) async throws {
    let surveyDataURL = localSurveyDataURL(forYear: year)
    if FileManager.default.fileExists(atPath: surveyDataURL.path) {
      // survey already exists. skipping.
      return
    }
    let surveyURL = remoteSurveyDataZipURL(forYear: year)
    let (zipFileURL, _) = try await URLSession.shared.download(from: surveyURL)
    try FileManager.default.createDirectory(
      at: surveyDataURL, withIntermediateDirectories: true)
    try Shell.execute("unzip '\(zipFileURL.path)' -d '\(surveyDataURL.path)'")
  }
  
  private static func localSurveyDataURL(forYear year: Int) -> URL {
    localDataURL.appending(component: "\(year)")
  }
  
  private static func remoteSurveyDataZipURL(forYear year: Int) -> URL {
    URL(string: "https://info.stackoverflowsolutions.com/rs/719-EMH-566/images/stack-overflow-developer-survey-\(year).zip")!
  }
}

fileprivate enum Shell {
  
  @discardableResult
  static func execute(_ command: String) throws -> String {
    let process = Process()
    let pipe = Pipe()
    process.executableURL = URL(fileURLWithPath: "/bin/zsh")
    process.arguments = ["-c", command]
    process.standardOutput = pipe
    process.standardError = pipe
    try process.run()
    process.waitUntilExit()
    let data = pipe.fileHandleForReading.readDataToEndOfFile()
    return String(data: data, encoding: .utf8)!
  }
}
