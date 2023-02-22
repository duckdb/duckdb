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
  
  func test_extract_from_uuid() throws {
    let expected = [
      UUID(uuidString: "00000000-0000-0000-0000-000000000001"),
      UUID(uuidString: "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF"),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT uuid FROM test_all_types();")
    let column = result[0].cast(to: UUID.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_time() throws {
    let t1 = Time.Components(hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Time.Components(hour: 23, minute: 59, second: 59, microsecond: 999_999)
    let expected = [
      Time(components: t1),
      Time(components: t2),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT time FROM test_all_types();")
    let column = result[0].cast(to: Time.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_time_tz() throws {
    let t1 = Time.Components(hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Time.Components(hour: 23, minute: 59, second: 59, microsecond: 999_999)
    let expected = [
      Time(components: t1),
      Time(components: t2),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT time_tz FROM test_all_types();")
    let column = result[0].cast(to: Time.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_date() throws {
    let d1 = Date.Components(year: -5_877_641, month: 06, day: 25)
    let d2 = Date.Components(year: 5_881_580, month: 07, day: 10)
    let expected = [
      Date(components: d1),
      Date(components: d2),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT date FROM test_all_types();")
    let column = result[0].cast(to: Date.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_timestamp() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 775_806)
    let expected = [
      Timestamp(components: t1),
      Timestamp(components: t2),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT timestamp FROM test_all_types();")
    let column = result[0].cast(to: Timestamp.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_timestamp_tz() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 775_806)
    let expected = [
      Timestamp(components: t1),
      Timestamp(components: t2),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT timestamp_tz FROM test_all_types();")
    let column = result[0].cast(to: Timestamp.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_timestamp_s() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 0)
    let expected = [
      Timestamp(components: t1),
      Timestamp(components: t2),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT timestamp_s FROM test_all_types();")
    let column = result[0].cast(to: Timestamp.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_timestamp_ms() throws {
    let t1 = Timestamp.Components(
      year: -290_308, month: 12, day: 22, hour: 0, minute: 0, second: 0, microsecond: 0)
    let t2 = Timestamp.Components(
      year: 294_247, month: 01, day: 10, hour: 04, minute: 0, second: 54, microsecond: 775_000)
    let expected = [
      Timestamp(components: t1),
      Timestamp(components: t2),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT timestamp_ms FROM test_all_types();")
    let column = result[0].cast(to: Timestamp.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_timestamp_ns() throws {
    let t1 = Timestamp.Components(
      year: 1677, month: 09, day: 21, hour: 0, minute: 12, second: 43, microsecond: 145_225)
    let t2 = Timestamp.Components(
      year: 2262, month: 04, day: 11, hour: 23, minute: 47, second: 16, microsecond: 854_775)
    let expected = [
      Timestamp(components: t1),
      Timestamp(components: t2),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT timestamp_ns FROM test_all_types();")
    let column = result[0].cast(to: Timestamp.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_interval() throws {
    let expected = [
      Interval(
        years: 0, months: 0, days: 0, hours: 0, minutes: 0, seconds: 0, microseconds: 0),
      Interval(
        years: 83, months: 3, days: 999, hours: 0, minutes: 16, seconds: 39, microseconds: 999_999),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT interval FROM test_all_types();")
    let column = result[0].cast(to: Interval.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_blob() throws {
    let expected = [
      "thisisalongblob\0withnullbytes".data(using: .ascii)!,
      "\0\0\0a".data(using: .ascii)!,
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT blob FROM test_all_types();")
    let column = result[0].cast(to: Data.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_decimal_4_1() throws {
    let expected = [
      Decimal(string: "-999.9"),
      Decimal(string: " 999.9"),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT dec_4_1 FROM test_all_types();")
    let column = result[0].cast(to: Decimal.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_decimal_9_4() throws {
    let expected = [
      Decimal(string: "-99999.9999"),
      Decimal(string: " 99999.9999"),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT dec_9_4 FROM test_all_types();")
    let column = result[0].cast(to: Decimal.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_decimal_18_6() throws {
    let expected = [
      Decimal(string: "-999999999999.999999"),
      Decimal(string:  "999999999999.999999"),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT dec_18_6 FROM test_all_types();")
    let column = result[0].cast(to: Decimal.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_decimal_38_10() throws {
    let expected = [
      Decimal(string: "-9999999999999999999999999999.9999999999"),
      Decimal(string:  "9999999999999999999999999999.9999999999"),
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT dec38_10 FROM test_all_types();")
    let column = result[0].cast(to: Decimal.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
  
  func test_extract_from_hugeint() throws {
    let expected = [
      IntHuge.min + 1,
      IntHuge.max,
      nil
    ]
    let connection = try Database(store: .inMemory).connect()
    let result = try connection.query("SELECT hugeint FROM test_all_types();")
    let column = result[0].cast(to: IntHuge.self)
    for (index, item) in expected.enumerated() {
      XCTAssertEqual(column[DBInt(index)], item)
    }
  }
}

