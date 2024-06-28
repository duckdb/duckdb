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

import SwiftUI

@main
struct ExplorerApp: App {
  
  private enum ViewState {
    case loading(Error?)
    case ready(ExoplanetStore)
  }

  @State private var state = ViewState.loading(nil)
  
  var body: some Scene {
    WindowGroup {
      Group {
        switch state {
        case .ready(let exoplanetStore):
          ContentView(exoplanetStore: exoplanetStore)
        case .loading(nil):
          ProgressView { Text("Loading Exoplanets") }
        case .loading(let error?):
          ErrorView(title: "Failed To Load Exoplanets", error: error) {
            Task { await prepareExoplanetsStore() }
          }
        }
      }
      .task {
        await prepareExoplanetsStore()
      }
    }
  }
  
  private func prepareExoplanetsStore() async {
    guard case .loading(_) = state else { return }
    self.state = .loading(nil)
    do {
      self.state = .ready(try await ExoplanetStore.create())
    }
    catch {
      self.state = .loading(error)
    }
  }
}
