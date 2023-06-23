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

final class PreparedStatementTests: XCTestCase {
  
  func test_bool_round_trip() throws {
    try roundTripTest(
      dataType: "BOOL",
      expected: [false, true, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Bool.self) }
    )
  }
  
  func test_utinyint_round_trip() throws {
    try roundTripTest(
      dataType: "UTINYINT",
      expected: [UInt8.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: UInt8.self) }
    )
  }
  
  func test_usmallint_round_trip() throws {
    try roundTripTest(
      dataType: "USMALLINT",
      expected: [UInt16.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: UInt16.self) }
    )
  }
  
  func test_uint_round_trip() throws {
    try roundTripTest(
      dataType: "UINTEGER",
      expected: [UInt32.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: UInt32.self) }
    )
  }
  
  func test_ubigint_round_trip() throws {
    try roundTripTest(
      dataType: "UBIGINT",
      expected: [UInt64.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: UInt64.self) }
    )
  }
  
  func test_tinyint_round_trip() throws {
    try roundTripTest(
      dataType: "TINYINT",
      expected: [Int8.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Int8.self) }
    )
  }
  
  func test_smallint_round_trip() throws {
    try roundTripTest(
      dataType: "SMALLINT",
      expected: [Int16.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Int16.self) }
    )
  }
  
  func test_int_round_trip() throws {
    try roundTripTest(
      dataType: "INTEGER",
      expected: [Int32.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Int32.self) }
    )
  }
  
  func test_bigint_round_trip() throws {
    try roundTripTest(
      dataType: "BIGINT",
      expected: [Int64.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Int64.self) }
    )
  }
  
  func test_hugeint_round_trip() throws {
    try roundTripTest(
      dataType: "HUGEINT",
      expected: [IntHuge.min, .max, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: IntHuge.self) }
    )
  }
  
  func test_float_round_trip() throws {
    try roundTripTest(
      dataType: "FLOAT",
      expected: [-Float.greatestFiniteMagnitude, .greatestFiniteMagnitude, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Float.self) }
    )
  }
  
  func test_double_round_trip() throws {
    try roundTripTest(
      dataType: "DOUBLE",
      expected: [-Double.greatestFiniteMagnitude, .greatestFiniteMagnitude, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Double.self) }
    )
  }
  
  func test_varchar_round_trip() throws {
    try roundTripTest(
      dataType: "VARCHAR",
      expected: ["🦆🦆🦆🦆🦆🦆", "goo\0se", nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: String.self) }
    )
  }
  
  func test_time_round_trip() throws {
    let t1 = Time.Components(hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Time.Components(hour: 23, minute: 59, second: 59, microsecond: 999_999)
    try roundTripTest(
      dataType: "TIME",
      expected: [Time(components: t1), Time(components: t2), nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Time.self) }
    )
  }
  
  func test_date_round_trip() throws {
    let d1 = Date.Components(year: -5_877_641, month: 06, day: 25)
    let d2 = Date.Components(year: 5_881_580, month: 07, day: 10)
    try roundTripTest(
      dataType: "DATE",
      expected: [Date(components: d1), Date(components: d2), nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Date.self) }
    )
  }
  
  func test_timestamp_round_trip() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 775_806)
    try roundTripTest(
      dataType: "TIMESTAMP",
      expected: [Timestamp(components: t1), Timestamp(components: t2), nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Timestamp.self) }
    )
  }
  
  func test_interval_round_trip() throws {
    let t1 = Interval(
      years: 0, months: 0, days: 0, hours: 0, minutes: 0, seconds: 0, microseconds: 0)
    let t2 = Interval(
      years: 83, months: 3, days: 999, hours: 0, minutes: 16, seconds: 39, microseconds: 999_999)
    try roundTripTest(
      dataType: "INTERVAL",
      expected: [t1, t2, nil],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Interval.self) }
    )
  }
  
  func test_blob_round_trip() throws {
    try roundTripTest(
      dataType: "BLOB",
      expected: [
        "thisisalongblob\0withnullbytes".data(using: .ascii)!,
        "\0\0\0a".data(using: .ascii)!,
        nil
      ],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Data.self) }
    )
  }
  
  func test_decimal_4_1_roundtrip() throws {
    try roundTripTest(
      dataType: "DECIMAL(4,1)",
      expected: [
        Decimal(string: "-999.9"),
        Decimal(string: " 999.9"),
        nil
      ],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Decimal.self) }
    )
  }
  
  func test_decimal_9_4_roundtrip() throws {
    try roundTripTest(
      dataType: "DECIMAL(9,4)",
      expected: [
        Decimal(string: "-99999.9999"),
        Decimal(string: " 99999.9999"),
        nil
      ],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Decimal.self) }
    )
  }
  
  func test_decimal_18_6_roundtrip() throws {
    try roundTripTest(
      dataType: "DECIMAL(18,6)",
      expected: [
        Decimal(string: "-999999999999.999999"),
        Decimal(string:  "999999999999.999999"),
        nil
      ],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Decimal.self) }
    )
  }
  
  func test_decimal_38_10_roundtrip() throws {
    try roundTripTest(
      dataType: "DECIMAL(38,10)",
      expected: [
        Decimal(string: "-9999999999999999999999999999.9999999999"),
        Decimal(string:  "9999999999999999999999999999.9999999999"),
        nil
      ],
      bind: { statement, item in try statement.bind(item, at: 1) },
      cast: { $0.cast(to: Decimal.self) }
    )
  }
}

private extension PreparedStatementTests {
  
  func roundTripTest<T: Equatable>(
    dataType: String,
    expected: [T?],
    bind: (PreparedStatement, T?) throws -> Void,
    cast: (Column<Void>) -> Column<T>
  ) throws {
    let connection = try Database(store: .inMemory).connect()
    try connection.execute("CREATE TABLE t1(i \(dataType));")
    let statement = try PreparedStatement(
      connection: connection, query: "INSERT INTO t1 VALUES ($1);")
    for item in expected {
      try bind(statement, item)
      let _ = try statement.execute()
    }
    let result = try connection.query("SELECT * FROM t1;")
    let column = cast(result[0])
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
}
