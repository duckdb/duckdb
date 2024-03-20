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

import XCTest
@testable import DuckDB

final class CodingUserInfoKeysTests: XCTestCase {
  func test_logical_type() throws {
    let connection = try Database(store: .inMemory).connect()
    try connection.execute("""
      CREATE TABLE t1(int_list INT[]);
      INSERT INTO t1 VALUES ([1, 2, 3]);
    """)
    let result = try connection.query("SELECT * FROM t1;")
    let column = result[0].cast(to: TestDynamicDecodable.self)
    XCTAssertEqual(column[0]?.logicalType?.dataType, .list)
  }
}

struct TestDynamicDecodable: Decodable {
  let logicalType: LogicalType?

  init(from decoder: Decoder) throws {
    self.logicalType = decoder.userInfo[CodingUserInfoKeys.logicalTypeCodingUserInfoKey] as? LogicalType
  }
}
