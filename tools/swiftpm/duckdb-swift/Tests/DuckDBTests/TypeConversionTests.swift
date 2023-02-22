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

final class TypeConversionTests: XCTestCase {
  
  func test_extract_from_bool() throws {
    let expected = [false, true, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT bool FROM test_all_types();")
    let column = result[0].cast(to: Bool.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_utinyint() throws {
    let expected = [UInt8.min, .max, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT utinyint FROM test_all_types();")
    let column = result[0].cast(to: UInt8.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_usmallint() throws {
    let expected = [UInt16.min, .max, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT usmallint FROM test_all_types();")
    let column = result[0].cast(to: UInt16.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_uint() throws {
    let expected = [UInt32.min, .max, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT uint FROM test_all_types();")
    let column = result[0].cast(to: UInt32.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_ubigint() throws {
    let expected = [UInt64.min, .max, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT ubigint FROM test_all_types();")
    let column = result[0].cast(to: UInt64.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_tinyint() throws {
    let expected = [Int8.min, .max, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT tinyint FROM test_all_types();")
    let column = result[0].cast(to: Int8.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_smallint() throws {
    let expected = [Int16.min, .max, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT smallint FROM test_all_types();")
    let column = result[0].cast(to: Int16.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_int() throws {
    let expected = [Int32.min, .max, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT int FROM test_all_types();")
    let column = result[0].cast(to: Int32.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_bigint() throws {
    let expected = [Int64.min, .max, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT bigint FROM test_all_types();")
    let column = result[0].cast(to: Int64.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_float() throws {
    let expected = [-Float.greatestFiniteMagnitude, .greatestFiniteMagnitude, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT float FROM test_all_types();")
    let column = result[0].cast(to: Float.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_double() throws {
    let expected = [-Double.greatestFiniteMagnitude, .greatestFiniteMagnitude, nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT double FROM test_all_types();")
    let column = result[0].cast(to: Double.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_varchar() throws {
    let expected = ["🦆🦆🦆🦆🦆🦆", "goo\0se", nil]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT varchar FROM test_all_types();")
    let column = result[0].cast(to: String.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
}

