//
//  DuckDB
//  https://github.com/duckdb/duckdb-swift
//
//  Copyright © 2018-2024 Stichting DuckDB Foundation
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

import Charts
import SwiftUI
@preconcurrency import TabularData

struct ContentView: View {
  
  private enum ViewState {
    case fetching(Error?)
    case loaded(DataFrame)
  }
  
  let exoplanetStore: ExoplanetStore
  @State private var state = ViewState.fetching(nil)

  var body: some View {
    Group {
      switch state {
      case .loaded(let dataFrame):
        VStack {
          Text("Number of Exoplanets Discovered By Year")
            .font(.title)
            .fontDesign(.default)
            .fontWeight(.bold)
            .multilineTextAlignment(.center)
          DiscoveryYearChart(dataFrame: dataFrame)
        }
      case .fetching(nil):
        ProgressView { Text("Fetching Data") }
      case .fetching(let error?):
        ErrorView(title: "Query Failed", error: error)
      }
    }
    .padding()
    .task {
      do {
        let frame = try await exoplanetStore.groupedByDiscoveryYear()
        self.state = .loaded(frame)
      }
      catch {
        self.state = .fetching(error)
      }
    }
  }
}

// MARK: - Chart View

struct DiscoveryYearChart: View {
  
  private struct ChartRow {
    let year: Date
    let count: Int
  }
  
  let dataFrame: DataFrame
  
  private var rows: [ChartRow] {
    let yearColumn = dataFrame.columns[0].assumingType(Int.self).filled(with: 9999)
    let countColumn = dataFrame.columns[1].assumingType(Int.self).filled(with: -1)
    let calendar = Calendar(identifier: .gregorian)
    var rows = [ChartRow]()
    for (year, count) in zip(yearColumn, countColumn) {
      let dateComponents = DateComponents(calendar: calendar, year: year)
      let date = dateComponents.date ?? .distantPast
      rows.append(ChartRow(year: date, count: count))
    }
    return rows
  }
  
  var body: some View {
    Chart(rows, id: \.year) { row in
      BarMark(
        x: .value("Year", row.year, unit: .year),
        y: .value("Count", row.count)
      )
    }
  }
}
