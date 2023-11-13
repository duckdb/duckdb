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


import Foundation
import XCTest
@testable import DuckDB

final class ExtensionTests: XCTestCase {
  func test_parquet() throws {
    try loadExtension(extension_name: "parquet")
  }
  func test_json() throws {
    try loadExtension(extension_name: "json")
  }
  func test_icu() throws {
    try loadExtension(extension_name: "icu")
  }
}

private extension ExtensionTests {
  func loadExtension(extension_name: String) throws {
    let connection = try Database(store: .inMemory).connect()

    try connection.execute("LOAD \(extension_name)")

    let prepped = try PreparedStatement(
        connection: connection,
        query: "SELECT installed, loaded FROM duckdb_extensions() where extension_name = $1"
    )

    try prepped.bind(extension_name, at: 1);

    let result = try prepped.execute()

    XCTAssertEqual(result[0].cast(to: Bool.self)[0], true)
    XCTAssertEqual(result[1].cast(to: Bool.self)[0], true)
  }
}
