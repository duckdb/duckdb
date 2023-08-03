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

import XCTest
@testable import DuckDB

final class DatabaseTests: XCTestCase {
  
  func test_create_in_memory_database() throws {
    let _ = try Database(store: .inMemory)
  }
  
  func test_create_local_file_datebase() throws {
    let fileURL = try Self.generateTemporaryFileURL(forFileNamed: "test.tb")
    defer { Self.cleanUpTemporaryFileURL(fileURL) }
    let _ = try Database(store: .file(at: fileURL))
  }
  
  func test_connect_to_in_memory_datebase() throws {
    let _ = try Database(store: .inMemory).connect()
  }
  
  func test_connect_to_local_file_datebase() throws {
    let fileURL = try Self.generateTemporaryFileURL(forFileNamed: "test.tb")
    defer { Self.cleanUpTemporaryFileURL(fileURL) }
    let _ = try Database(store: .file(at: fileURL)).connect()
  }
  
  func test_execute_statement() throws {
    let connection = try Database(store: .inMemory).connect()
    try connection.execute("SELECT version();")
  }
  
  func test_query_result() throws {
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT * FROM test_all_types(use_large_enum=true);")
    let element: Void? = result[0][0]
    XCTAssertNotNil(element)
  }
}

// MARK: - Temp Directory Helpers

private extension DatabaseTests {
  
  static func generateTemporaryFileURL(forFileNamed fileName: String) throws -> URL {
    let tmpDirURL = try FileManager.default.url(
      for: .itemReplacementDirectory,
      in: .userDomainMask,
      appropriateFor: FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0],
      create: true
    )
    return tmpDirURL.appendingPathComponent(fileName)
  }
  
  static func cleanUpTemporaryFileURL(_ fileURL: URL) {
    do {
      try FileManager.default.removeItem(at: fileURL.deletingLastPathComponent())
    }
    catch {
      print("Ignored failed attempt to remove temp dir at \(fileURL):\n\t\(error)")
    }
  }
}
