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

import Foundation
import XCTest
@testable import DuckDB

final class LogicalTypeTests: XCTestCase {

  func test_boolean() throws {
    try logicalTypeTest(
      dataType: "BOOL",
      cast: { $0.cast(to: Bool.self) },
      validate: {
        XCTAssertEqual($0.dataType, .boolean)
        XCTAssertEqual($0.underlyingDataType, .boolean)
      }
    )
  }

  func test_tinyint() throws {
    try logicalTypeTest(
      dataType: "TINYINT",
      cast: { $0.cast(to: Int8.self) },
      validate: {
        XCTAssertEqual($0.dataType, .tinyint)
        XCTAssertEqual($0.underlyingDataType, .tinyint)
      }
    )
  }

  func test_smallint() throws {
    try logicalTypeTest(
      dataType: "SMALLINT",
      cast: { $0.cast(to: Int16.self) },
      validate: {
        XCTAssertEqual($0.dataType, .smallint)
        XCTAssertEqual($0.underlyingDataType, .smallint)
      }
    )
  }

  func test_integer() throws {
    try logicalTypeTest(
      dataType: "INTEGER",
      cast: { $0.cast(to: Int32.self) },
      validate: {
        XCTAssertEqual($0.dataType, .integer)
        XCTAssertEqual($0.underlyingDataType, .integer)
      }
    )
  }

  func test_bigint() throws {
    try logicalTypeTest(
      dataType: "BIGINT",
      cast: { $0.cast(to: Int64.self) },
      validate: {
        XCTAssertEqual($0.dataType, .bigint)
        XCTAssertEqual($0.underlyingDataType, .bigint)
      }
    )
  }

  func test_hugeint() throws {
    try logicalTypeTest(
      dataType: "HUGEINT",
      cast: { $0.cast(to: IntHuge.self) },
      validate: {
        XCTAssertEqual($0.dataType, .hugeint)
        XCTAssertEqual($0.underlyingDataType, .hugeint)
      }
    )
  }

  func test_uhugeint() throws {
    try logicalTypeTest(
      dataType: "UHUGEINT",
      cast: { $0.cast(to: UIntHuge.self) },
      validate: {
        XCTAssertEqual($0.dataType, .uhugeint)
        XCTAssertEqual($0.underlyingDataType, .uhugeint)
      }
    )
  }

  func test_utinyint() throws {
    try logicalTypeTest(
      dataType: "UTINYINT",
      cast: { $0.cast(to: UInt8.self) },
      validate: {
        XCTAssertEqual($0.dataType, .utinyint)
        XCTAssertEqual($0.underlyingDataType, .utinyint)
      }
    )
  }

  func test_usmallint() throws {
    try logicalTypeTest(
      dataType: "USMALLINT",
      cast: { $0.cast(to: UInt16.self) },
      validate: {
        XCTAssertEqual($0.dataType, .usmallint)
        XCTAssertEqual($0.underlyingDataType, .usmallint)
      }
    )
  }

  func test_uinteger() throws {
    try logicalTypeTest(
      dataType: "UINTEGER",
      cast: { $0.cast(to: UInt32.self) },
      validate: {
        XCTAssertEqual($0.dataType, .uinteger)
        XCTAssertEqual($0.underlyingDataType, .uinteger)
      }
    )
  }

  func test_ubigint() throws {
    try logicalTypeTest(
      dataType: "UBIGINT",
      cast: { $0.cast(to: UInt64.self) },
      validate: {
        XCTAssertEqual($0.dataType, .ubigint)
        XCTAssertEqual($0.underlyingDataType, .ubigint)
      }
    )
  }

  func test_float() throws {
    try logicalTypeTest(
      dataType: "FLOAT",
      cast: { $0.cast(to: Float.self) },
      validate: {
        XCTAssertEqual($0.dataType, .float)
        XCTAssertEqual($0.underlyingDataType, .float)
      }
    )
  }

  func test_double() throws {
    try logicalTypeTest(
      dataType: "DOUBLE",
      cast: { $0.cast(to: Double.self) },
      validate: {
        XCTAssertEqual($0.dataType, .double)
        XCTAssertEqual($0.underlyingDataType, .double)
      }
    )
  }

  func test_timestamp() throws {
    try logicalTypeTest(
      dataType: "TIMESTAMP",
      cast: { $0.cast(to: Timestamp.self) },
      validate: {
        XCTAssertEqual($0.dataType, .timestamp)
        XCTAssertEqual($0.underlyingDataType, .timestamp)
      }
    )
  }

  func test_timestamp_s() throws {
    try logicalTypeTest(
      dataType: "TIMESTAMP_S",
      cast: { $0.cast(to: Timestamp.self) },
      validate: {
        XCTAssertEqual($0.dataType, .timestampS)
        XCTAssertEqual($0.underlyingDataType, .timestampS)
      }
    )
  }

  func test_timestamp_ms() throws {
    try logicalTypeTest(
      dataType: "TIMESTAMP_MS",
      cast: { $0.cast(to: Timestamp.self) },
      validate: {
        XCTAssertEqual($0.dataType, .timestampMS)
        XCTAssertEqual($0.underlyingDataType, .timestampMS)
      }
    )
  }

  func test_timestamp_ns() throws {
    try logicalTypeTest(
      dataType: "TIMESTAMP_NS",
      cast: { $0.cast(to: Timestamp.self) },
      validate: {
        XCTAssertEqual($0.dataType, .timestampNS)
        XCTAssertEqual($0.underlyingDataType, .timestampNS)
      }
    )
  }

  func test_date() throws {
    try logicalTypeTest(
      dataType: "DATE",
      cast: { $0.cast(to: Date.self) },
      validate: {
        XCTAssertEqual($0.dataType, .date)
        XCTAssertEqual($0.underlyingDataType, .date)
      }
    )
  }

  func test_time() throws {
    try logicalTypeTest(
      dataType: "TIME",
      cast: { $0.cast(to: Time.self) },
      validate: {
        XCTAssertEqual($0.dataType, .time)
        XCTAssertEqual($0.underlyingDataType, .time)
      }
    )
  }

  func test_interval() throws {
    try logicalTypeTest(
      dataType: "INTERVAL",
      cast: { $0.cast(to: Interval.self) },
      validate: {
        XCTAssertEqual($0.dataType, .interval)
        XCTAssertEqual($0.underlyingDataType, .interval)
      }
    )
  }

  func test_varchar() throws {
    try logicalTypeTest(
      dataType: "VARCHAR",
      cast: { $0.cast(to: String.self) },
      validate: {
        XCTAssertEqual($0.dataType, .varchar)
        XCTAssertEqual($0.underlyingDataType, .varchar)
      }
    )
  }

  func test_blob() throws {
    try logicalTypeTest(
      dataType: "BLOB",
      cast: { $0.cast(to: Data.self) },
      validate: {
        XCTAssertEqual($0.dataType, .blob)
        XCTAssertEqual($0.underlyingDataType, .blob)
      }
    )
  }

  func test_decimal() throws {
    try logicalTypeTest(
      dataType: "DECIMAL(38, 10)",
      cast: { $0.cast(to: Decimal.self) },
      validate: {
        XCTAssertEqual($0.dataType, .decimal)
        XCTAssertEqual($0.underlyingDataType, .decimal)

        let properties = $0.decimalProperties!
        XCTAssertEqual(properties.scale, 10)
        XCTAssertEqual(properties.width, 38)
        XCTAssertEqual(properties.storageType, .hugeint)
      }
    )
  }

  enum Mood: String, Equatable, Decodable {
    case sad
    case ok
    case happy
  }

  func test_enum() throws {
    try logicalTypeTest(
      dataType: "mood",
      cast: { $0.cast(to: Mood.self) },
      before: { connection in
        try connection.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")
      },
      validate: {
        XCTAssertEqual($0.dataType, .enum)
        XCTAssertEqual($0.underlyingDataType, .utinyint)
      }
    )
  }

  func test_list() throws {
    try logicalTypeTest(
      dataType: "INT[]",
      cast: { $0.cast(to: [Int32?].self) },
      validate: {
        XCTAssertEqual($0.dataType, .list)
        XCTAssertEqual($0.underlyingDataType, .list)
        XCTAssertEqual($0.listChildType?.dataType, .integer)
      }
    )
  }

  struct NumStrStruct: Equatable, Decodable {
    let num: Int32
    let str: String
  }

  func test_struct() throws {
    let connection = try Database(store: .inMemory).connect()
    try connection.execute("CREATE TABLE t1(num INTEGER, str VARCHAR);")
    let result = try connection.query("SELECT {'num': num, 'str': str} as struct_column FROM t1;")
    let logicalType = result[0].cast(to: NumStrStruct.self).underlyingLogicalType

    XCTAssertEqual(logicalType.dataType, .struct)
    XCTAssertEqual(logicalType.underlyingDataType, .struct)

    let properties = logicalType.structMemberProperties!
    XCTAssertEqual(properties.count, 2)
    XCTAssertEqual(properties[0].name, "num")
    XCTAssertEqual(properties[0].type.dataType, .integer)
    XCTAssertEqual(properties[1].name, "str")
    XCTAssertEqual(properties[1].type.dataType, .varchar)
  }

  func test_map() throws {
    try logicalTypeTest(
      dataType: "MAP(INT,DOUBLE)",
      cast: { $0.cast(to: [Int32: Double].self) },
      validate: {
        XCTAssertEqual($0.dataType, .map)
        XCTAssertEqual($0.underlyingDataType, .map)
        XCTAssertEqual($0.mapKeyType?.dataType, .integer)
        XCTAssertEqual($0.mapValueType?.dataType, .double)
      }
    )
  }

  enum NumStrUnion: Equatable, Decodable {
    case num(Int32)
    case str(String)
  }

  func test_union() throws {
    try logicalTypeTest(
      dataType: "UNION(num INT, str VARCHAR)",
      cast: { $0.cast(to: NumStrUnion.self) },
      validate: {
        XCTAssertEqual($0.dataType, .union)
        XCTAssertEqual($0.underlyingDataType, .union)

        let properties = $0.unionMemberProperties!
        XCTAssertEqual(properties.count, 2)
        XCTAssertEqual(properties[0].name, "num")
        XCTAssertEqual(properties[0].type.dataType, .integer)
        XCTAssertEqual(properties[1].name, "str")
        XCTAssertEqual(properties[1].type.dataType, .varchar)
      }
    )
  }

  func test_uuid() throws {
    try logicalTypeTest(
      dataType: "UUID",
      cast: { $0.cast(to: UUID.self) },
      validate: {
        XCTAssertEqual($0.dataType, .uuid)
        XCTAssertEqual($0.underlyingDataType, .uuid)
      }
    )
  }
}

private extension LogicalTypeTests {

  func logicalTypeTest<T: Equatable>(
    dataType: String,
    cast: (Column<Void>) -> Column<T>,
    before: ((Connection) throws -> Void)? = nil,
    validate: (LogicalType) -> Void
  ) throws {
    let connection = try Database(store: .inMemory).connect()
    try before?(connection)
    try connection.execute("CREATE TABLE t1(i \(dataType));")
    let result = try connection.query("SELECT * FROM t1;")
    validate(cast(result[0]).underlyingLogicalType)
  }
}
