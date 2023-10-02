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
/*
  FIXME: TIMETZ <> TIME
  func test_extract_from_time_tz() throws {
    let expected = [
      Time(components: .init(hour: 0, minute: 0, second: 0, microsecond: 0)),
      Time(components: .init(hour: 23, minute: 59, second: 59, microsecond: 999_999)),
      nil
    ]
    try extractTest(testColumnName: "time_tz", expected: expected) { $0.cast(to: Time.self) }
  }
*/
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
  
  func test_extract_from_enum_small() throws {
    enum SmallEnum: UInt8, RawRepresentable, Decodable {
      case duckDuckEnum
      case goose
    }
    let expected = [SmallEnum.duckDuckEnum, .goose, nil]
    try extractTest(
      testColumnName: "small_enum", expected: expected) { $0.cast(to: SmallEnum.self) }
  }
  
  func test_extract_from_enum_medium() throws {
    enum SmallEnum: UInt16, RawRepresentable, Decodable {
      case enum0
      case enum299 = 299
    }
    let expected = [SmallEnum.enum0, .enum299, nil]
    try extractTest(
      testColumnName: "medium_enum", expected: expected) { $0.cast(to: SmallEnum.self) }
  }
  
  func test_extract_from_enum_large() throws {
    enum SmallEnum: UInt32, RawRepresentable, Decodable {
      case enum0
      case enum69_999 = 69_999
    }
    let expected = [SmallEnum.enum0, .enum69_999, nil]
    try extractTest(
      testColumnName: "large_enum", expected: expected) { $0.cast(to: SmallEnum.self) }
  }
  
  func test_extract_from_int_array() throws {
    let expected = [[], [Int32(42), 999, nil, nil, -42], nil]
    try extractTest(testColumnName: "int_array", expected: expected) { $0.cast(to: [Int32?].self) }
  }
  
  func test_extract_from_int_array_casting_to_swift_int() throws {
    let expected = [[], [Int(42), 999, nil, nil, -42], nil]
    try extractTest(testColumnName: "int_array", expected: expected) { $0.cast(to: [Int?].self) }
  }
  
  func test_extract_from_double_array() throws {
    // We need this contraption to work around .nan != .nan
    enum DoubleBox: Equatable {
      case normal(Double?)
      case nan
      init(_ source: Double?) {
        switch source {
        case let source? where source.isNaN:
          self = .nan
        case let source:
          self = .normal(source)
        }
      }
    }
    let source = [[], [Double(42), .nan, .infinity, -.infinity, nil, -42], nil]
    let expected = source.map { $0?.map(DoubleBox.init(_:)) }
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT double_array FROM test_all_types(use_large_enum=true);")
    let column = result[0].cast(to: [Double?].self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)]?.map(DoubleBox.init(_:)), item)
    }
  }
  
  func test_extract_from_date_array() throws {
    let expected = [
      [],
      [
        Date(components: .init(year: 1970, month: 01, day: 01)),
        Date(days: 2147483647),
        Date(days: -2147483647),
        nil,
        Date(components: .init(year: 2022, month: 05, day: 12)),
      ],
      nil
    ]
    try extractTest(
      testColumnName: "date_array", expected: expected
    ) { $0.cast(to: [DuckDB.Date?].self) }
  }
  
  func test_extract_from_timestamptz_array() throws {
    let t1 = Timestamp.Components(
      year: 1970, month: 01, day: 01, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 2022, month: 05, day: 12, hour: 23, minute: 23, second: 45, microsecond: 0)
    let expected = [
      [],
      [
        Timestamp(components: t1),
        Timestamp(microseconds: 9223372036854775807),
        Timestamp(microseconds: -9223372036854775807),
        nil,
        Timestamp(components: t2),
      ],
      nil
    ]
    try extractTest(
      testColumnName: "timestamptz_array", expected: expected
    ) { $0.cast(to: [Timestamp?].self) }
  }
  
  func test_extract_from_varchar_array() throws {
    let expected = [[], ["🦆🦆🦆🦆🦆🦆", "goose", nil, ""], nil]
    try extractTest(
      testColumnName: "varchar_array", expected: expected
    ) { $0.cast(to: [String?].self) }
  }
  
  func test_extract_from_nested_int_array() throws {
    let expected = [
      [],
      [[], [Int32(42), 999, nil, nil, -42], nil, [], [42, 999, nil, nil, -42]],
      nil
    ]
    try extractTest(
      testColumnName: "nested_int_array", expected: expected
    ) { $0.cast(to: [[Int32?]?].self) }
  }
  
  func test_extract_from_struct() throws {
    struct TestStruct: Decodable, Equatable {
      var a: Int32? = nil
      var b: String? = nil
    }
    let expected = [
      TestStruct(),
      TestStruct(a: 42, b: "🦆🦆🦆🦆🦆🦆"),
      nil
    ]
    try extractTest(testColumnName: "struct", expected: expected) { $0.cast(to: TestStruct.self) }
  }
  
  func test_extract_from_struct_of_arrays() throws {
    struct TestStruct: Decodable, Equatable {
      var a: [Int32?]? = nil
      var b: [String?]? = nil
    }
    let expected = [
      TestStruct(a: nil, b: nil),
      TestStruct(a: [42, 999, nil, nil, -42], b:  ["🦆🦆🦆🦆🦆🦆", "goose", nil, ""]),
      nil
    ]
    try extractTest(
      testColumnName: "struct_of_arrays", expected: expected) { $0.cast(to: TestStruct.self) }
  }
  
  func test_extract_from_array_of_structs() throws {
    struct TestStruct: Decodable, Equatable {
      var a: Int32? = nil
      var b: String? = nil
    }
    let expected = [
      [],
      [TestStruct(a: nil, b: nil), TestStruct(a: 42, b: "🦆🦆🦆🦆🦆🦆"), nil],
      nil
    ]
    try extractTest(
      testColumnName: "array_of_structs", expected: expected) { $0.cast(to: [TestStruct?].self) }
  }
  
  func test_extract_from_map() throws {
    let expected = [
      Dictionary(),
      Dictionary(uniqueKeysWithValues: [("key1", "🦆🦆🦆🦆🦆🦆"), ("key2", "goose")]),
      nil
    ]
    try extractTest(
      testColumnName: "map", expected: expected) { $0.cast(to: [String: String].self) }
  }
}

private extension TypeConversionTests {
  
  func extractTest<T: Equatable>(
    testColumnName: String,
    expected: [T?],
    cast: (Column<Void>) -> Column<T>
  ) throws {
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT \(testColumnName) FROM test_all_types(use_large_enum=true);")
    let column = cast(result[0])
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
}

