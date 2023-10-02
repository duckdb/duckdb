//
//  DuckDB
//  https://github.com/duckdb/duckdb-swift
//
//  Copyright © 2018-2023 Stichting DuckDB Foundation
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to
//  deal in the Software without restriction, including without limitation the
//  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
//  sell copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
//  IN THE SOFTWARE.

import DuckDB
import Foundation
import TabularData

final class ExoplanetStore {
  
  static func create() async throws -> ExoplanetStore {
    let (csvFileURL, _) = try await URLSession.shared.download(from: Self.csvRemoteURL)
    let database = try Database(store: .inMemory)
    let connection = try database.connect()
    let _ = try connection.query(
      "CREATE TABLE exoplanets AS SELECT * FROM read_csv_auto('\(csvFileURL.path)');")
    return ExoplanetStore(database: database, connection: connection)
  }
  
  let database: Database
  let connection: Connection
  
  private init(database: Database, connection: Connection) {
    self.database = database
    self.connection = connection
  }
  
  func groupedByDiscoveryYear() async throws -> DataFrame {
    let result = try connection.query("""
      SELECT disc_year, COUNT(disc_year) AS Count
        FROM exoplanets
        GROUP BY disc_year
        ORDER BY disc_year
      """)
    let discoveryYearColumn = result[0].cast(to: Int.self)
    let countColumn = result[1].cast(to: Int.self)
    return DataFrame(columns: [
      TabularData.Column(discoveryYearColumn)
        .eraseToAnyColumn(),
      TabularData.Column(countColumn)
        .eraseToAnyColumn(),
    ])
  }
}

private extension ExoplanetStore {
  
  static let csvRemoteURL: URL = {
    let apiEndpointURL = URL(
      string: "https://exoplanetarchive.ipac.caltech.edu/TAP/sync")!
    // column descriptions available at:
    // https://exoplanetarchive.ipac.caltech.edu/docs/API_PS_columns.html
    let remoteQueryColumns = [
      "pl_name",
      "hostname",
      "sy_snum",
      "disc_year",
      "disc_facility",
      "st_mass",
      "st_rad",
      "st_age",
    ]
    let remoteQuery = """
    SELECT+\(remoteQueryColumns.joined(separator: "+,+"))+FROM+pscomppars
    """
    return apiEndpointURL.appending(queryItems: [
      .init(name: "query", value: remoteQuery),
      .init(name: "format", value: "csv"),
    ])
  }()
}
