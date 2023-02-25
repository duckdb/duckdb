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

final class TypeConversionTests: XCTestCase {
  
  func test_extract_from_bool() throws {
    try extractTest(
      testColumnName: "bool", expected: [false, true, nil]) { $0.cast(to: Bool.self) }
  }
  
  func test_extract_from_utinyint() throws {
    try extractTest(
      testColumnName: "utinyint", expected: [UInt8.min, .max, nil]) { $0.cast(to: UInt8.self) }
  }
  
  func test_extract_from_usmallint() throws {
    try extractTest(
      testColumnName: "usmallint", expected: [UInt16.min, .max, nil]) { $0.cast(to: UInt16.self) }
  }
  
  func test_extract_from_uint() throws {
    try extractTest(
      testColumnName: "uint", expected: [UInt32.min, .max, nil]) { $0.cast(to: UInt32.self) }
  }
  
  func test_extract_from_ubigint() throws {
    try extractTest(
      testColumnName: "ubigint", expected: [UInt64.min, .max, nil]) { $0.cast(to: UInt64.self) }
  }
  
  func test_extract_from_tinyint() throws {
    try extractTest(
      testColumnName: "tinyint", expected: [Int8.min, .max, nil]) { $0.cast(to: Int8.self) }
  }
  
  func test_extract_from_smallint() throws {
    try extractTest(
      testColumnName: "smallint", expected: [Int16.min, .max, nil]) { $0.cast(to: Int16.self) }
  }
  
  func test_extract_from_int() throws {
    try extractTest(
      testColumnName: "int", expected: [Int32.min, .max, nil]) { $0.cast(to: Int32.self) }
  }
  
  func test_extract_from_bigint() throws {
    try extractTest(
      testColumnName: "bigint", expected: [Int64.min, .max, nil]) { $0.cast(to: Int64.self) }
  }
  
  func test_extract_from_hugeint() throws {
    let expected = [IntHuge.min + 1, IntHuge.max, nil]
    try extractTest(testColumnName: "hugeint", expected: expected) { $0.cast(to: IntHuge.self) }
  }
  
  func test_extract_from_float() throws {
    let expected = [-Float.greatestFiniteMagnitude, .greatestFiniteMagnitude, nil]
    try extractTest(testColumnName: "float", expected: expected) { $0.cast(to: Float.self) }
  }
  
  func test_extract_from_double() throws {
    let expected = [-Double.greatestFiniteMagnitude, .greatestFiniteMagnitude, nil]
    try extractTest(testColumnName: "double", expected: expected) { $0.cast(to: Double.self) }
  }
  
  func test_extract_from_varchar() throws {
    let expected = ["🦆🦆🦆🦆🦆🦆", "goo\0se", nil]
    try extractTest(testColumnName: "varchar", expected: expected) { $0.cast(to: String.self) }
  }
  
  func test_extract_from_uuid() throws {
    let expected = [
      UUID(uuidString: "00000000-0000-0000-0000-000000000001"),
      UUID(uuidString: "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
      nil
    ]
    try extractTest(testColumnName: "uuid", expected: expected) { $0.cast(to: UUID.self) }
  }
  
  func test_extract_from_time() throws {
    let expected = [
      Time(components: .init(hour: 0, minute: 0, second: 0, microsecond: 0)),
      Time(components: .init(hour: 23, minute: 59, second: 59, microsecond: 999_999)),
      nil
    ]
    try extractTest(testColumnName: "time", expected: expected) { $0.cast(to: Time.self) }
  }
  
  func test_extract_from_time_tz() throws {
    let expected = [
      Time(components: .init(hour: 0, minute: 0, second: 0, microsecond: 0)),
      Time(components: .init(hour: 23, minute: 59, second: 59, microsecond: 999_999)),
      nil
    ]
    try extractTest(testColumnName: "time_tz", expected: expected) { $0.cast(to: Time.self) }
  }
  
  func test_extract_from_date() throws {
    let expected = [
      Date(components: .init(year: -5_877_641, month: 06, day: 25)),
      Date(components: .init(year: 5_881_580, month: 07, day: 10)),
      nil
    ]
    try extractTest(testColumnName: "date", expected: expected) { $0.cast(to: Date.self) }
  }
  
  func test_extract_from_timestamp() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 775_806)
    let expected = [Timestamp(components: t1), Timestamp(components: t2), nil]
    try extractTest(testColumnName: "timestamp", expected: expected) { $0.cast(to: Timestamp.self) }
  }
  
  func test_extract_from_timestamp_tz() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 775_806)
    let expected = [Timestamp(components: t1), Timestamp(components: t2), nil]
    try extractTest(
      testColumnName: "timestamp_tz", expected: expected) { $0.cast(to: Timestamp.self) }
  }
  
  func test_extract_from_timestamp_s() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 0)
    let expected = [Timestamp(components: t1), Timestamp(components: t2), nil]
    try extractTest(
      testColumnName: "timestamp_s", expected: expected) { $0.cast(to: Timestamp.self) }
  }
  
  func test_extract_from_timestamp_ms() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 775_000)
    let expected = [Timestamp(components: t1), Timestamp(components: t2), nil]
    try extractTest(
      testColumnName: "timestamp_ms", expected: expected) { $0.cast(to: Timestamp.self) }
  }
  
  func test_extract_from_timestamp_ns() throws {
    let t1 = Timestamp.Components(
      year: 1677, month: 09, day: 21, hour: 0, minute: 12, second: 43, microsecond: 145_225)
    let t2 = Timestamp.Components(
      year: 2262, month: 04, day: 11, hour: 23, minute: 47, second: 16, microsecond: 854_775)
    let expected = [Timestamp(components: t1), Timestamp(components: t2), nil]
    try extractTest(
      testColumnName: "timestamp_ns", expected: expected) { $0.cast(to: Timestamp.self) }
  }
  
  func test_extract_from_interval() throws {
    let expected = [
      Interval(
        years: 0, months: 0, days: 0, hours: 0, minutes: 0, seconds: 0, microseconds: 0),
      Interval(
        years: 83, months: 3, days: 999, hours: 0, minutes: 16, seconds: 39, microseconds: 999_999),
      nil
    ]
    try extractTest(testColumnName: "interval", expected: expected) { $0.cast(to: Interval.self) }
  }
  
  func test_extract_from_blob() throws {
    let expected = [
      "thisisalongblob\0withnullbytes".data(using: .ascii)!,
      "\0\0\0a".data(using: .ascii)!,
      nil
    ]
    try extractTest(testColumnName: "blob", expected: expected) { $0.cast(to: Data.self) }
  }
  
  func test_extract_from_decimal_4_1() throws {
    let expected = [
      Decimal(string: "-999.9"),
      Decimal(string: " 999.9"),
      nil
    ]
    try extractTest(testColumnName: "dec_4_1", expected: expected) { $0.cast(to: Decimal.self) }
  }
  
  func test_extract_from_decimal_9_4() throws {
    let expected = [
      Decimal(string: "-99999.9999"),
      Decimal(string: " 99999.9999"),
      nil
    ]
    try extractTest(testColumnName: "dec_9_4", expected: expected) { $0.cast(to: Decimal.self) }
  }
  
  func test_extract_from_decimal_18_6() throws {
    let expected = [
      Decimal(string: "-999999999999.999999"),
      Decimal(string:  "999999999999.999999"),
      nil
    ]
    try extractTest(testColumnName: "dec_18_6", expected: expected) { $0.cast(to: Decimal.self) }
  }
  
  func test_extract_from_decimal_38_10() throws {
    let expected = [
      Decimal(string: "-9999999999999999999999999999.9999999999"),
      Decimal(string:  "9999999999999999999999999999.9999999999"),
      nil
    ]
    try extractTest(testColumnName: "dec38_10", expected: expected) { $0.cast(to: Decimal.self) }
  }
}

private extension TypeConversionTests {
  
  func extractTest<T: Equatable>(
    testColumnName: String,
    expected: [T?],
    cast: (Column<Void>) -> Column<T>
  ) throws {
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT \(testColumnName) FROM test_all_types();")
    let column = cast(result[0])
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
}

